use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream, StreamExt};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Dropbox storage adapter using OAuth2 access tokens.
///
/// File paths must start with "/" as per Dropbox API requirements.
#[derive(Clone, Debug)]
pub struct DropboxStorage {
    client: Client,
    access_token: SecretString,
}

impl DropboxStorage {
    const API_URL: &'static str = "https://api.dropboxapi.com/2";
    const CONTENT_URL: &'static str = "https://content.dropboxapi.com/2";

    /// Create a new Dropbox storage adapter with an access token.
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            access_token: SecretString::from(access_token.into()),
        }
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.access_token.expose_secret())
    }

    fn ensure_path_format(path: &str) -> String {
        if path.is_empty() || path == "/" {
            return String::new();
        }
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        }
    }

    fn map_error(&self, status: StatusCode, path: &str, body: &str) -> Error {
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(path.to_string()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::PermissionDenied(format!("Dropbox auth failed: {}", body))
            }
            StatusCode::CONFLICT => Error::Generic(format!("Dropbox conflict: {}", body)),
            StatusCode::TOO_MANY_REQUESTS => {
                Error::Generic("Dropbox rate limit exceeded".to_string())
            }
            _ => Error::Generic(format!("Dropbox error {}: {}", status, body)),
        }
    }
}

#[derive(Serialize)]
struct DropboxPath {
    path: String,
}

#[derive(Serialize)]
struct DropboxUploadArg {
    path: String,
    mode: String,
    autorename: bool,
    mute: bool,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct DropboxMetadata {
    #[serde(rename = ".tag")]
    tag: String,
    name: String,
    path_display: Option<String>,
}

#[derive(Serialize)]
struct DropboxListFolderArg {
    path: String,
    recursive: bool,
}

#[derive(Deserialize)]
struct DropboxListFolderResult {
    entries: Vec<DropboxEntry>,
    has_more: bool,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct DropboxEntry {
    #[serde(rename = ".tag")]
    tag: String,
    path_display: Option<String>,
}

impl Storage for DropboxStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let path = Self::ensure_path_format(id);

        let request_body = DropboxPath { path: path.clone() };

        let response = self
            .client
            .post(&format!("{}/files/get_metadata", Self::API_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        match status {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, &path, &body))
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        let path = Self::ensure_path_format(id);

        let request_body = DropboxPath { path: path.clone() };

        let response = self
            .client
            .post(&format!("{}/files/get_metadata", Self::API_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        match status {
            StatusCode::OK => {
                let body = response.text().await.unwrap_or_default();
                // Check if the metadata indicates it's a folder
                Ok(body.contains("\"folder\""))
            }
            StatusCode::NOT_FOUND => Ok(false),
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, &path, &body))
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        let path = Self::ensure_path_format(&id);

        // Read all data into memory
        // For large files, Dropbox supports chunked uploads which could be implemented
        let mut data = Vec::new();
        tokio::io::copy(&mut input, &mut data).await?;

        let upload_arg = DropboxUploadArg {
            path: path.clone(),
            mode: "overwrite".to_string(),
            autorename: false,
            mute: false,
        };

        let arg_json = serde_json::to_string(&upload_arg)
            .map_err(|e| Error::Generic(format!("JSON serialization error: {}", e)))?;

        let response = self
            .client
            .post(&format!("{}/files/upload", Self::CONTENT_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/octet-stream")
            .header("Dropbox-API-Arg", arg_json)
            .body(data)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(self.map_error(status, &path, &body))
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let path = Self::ensure_path_format(id);

        let download_arg = DropboxPath { path: path.clone() };

        let arg_json = serde_json::to_string(&download_arg)
            .map_err(|e| Error::Generic(format!("JSON serialization error: {}", e)))?;

        let response = self
            .client
            .post(&format!("{}/files/download", Self::CONTENT_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header("Dropbox-API-Arg", arg_json)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(self.map_error(status, &path, &body));
        }

        let mut stream = response.bytes_stream();
        let mut total_bytes = 0u64;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| Error::Connection(Box::new(e)))?;
            output.write_all(&chunk).await?;
            total_bytes += chunk.len() as u64;
        }

        output.flush().await?;
        Ok(total_bytes)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let path = Self::ensure_path_format(id);

        let request_body = DropboxPath { path: path.clone() };

        let response = self
            .client
            .post(&format!("{}/files/delete_v2", Self::API_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Ok(()), // Idempotent delete
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, &path, &body))
            }
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let path = match prefix {
            Some(p) if !p.is_empty() => Self::ensure_path_format(p),
            _ => String::new(),
        };

        let request_body = DropboxListFolderArg {
            path: path.clone(),
            recursive: true,
        };

        let response = self
            .client
            .post(&format!("{}/files/list_folder", Self::API_URL))
            .header(AUTHORIZATION, self.auth_header())
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(self.map_error(status, &path, &body));
        }

        let list_result: DropboxListFolderResult = response
            .json()
            .await
            .map_err(|e| Error::Generic(format!("Failed to parse list response: {}", e)))?;

        let mut all_entries = list_result.entries;
        let mut cursor = list_result.cursor;
        let mut has_more = list_result.has_more;

        // Continue fetching if there are more results
        while has_more {
            if let Some(ref c) = cursor {
                #[derive(Serialize)]
                struct ContinueArg {
                    cursor: String,
                }

                let continue_body = ContinueArg { cursor: c.clone() };

                let continue_response = self
                    .client
                    .post(&format!("{}/files/list_folder/continue", Self::API_URL))
                    .header(AUTHORIZATION, self.auth_header())
                    .header(CONTENT_TYPE, "application/json")
                    .json(&continue_body)
                    .send()
                    .await
                    .map_err(|e| Error::Connection(Box::new(e)))?;

                if !continue_response.status().is_success() {
                    break;
                }

                let continue_result: DropboxListFolderResult =
                    continue_response.json().await.map_err(|e| {
                        Error::Generic(format!("Failed to parse continue response: {}", e))
                    })?;

                all_entries.extend(continue_result.entries);
                cursor = continue_result.cursor;
                has_more = continue_result.has_more;
            } else {
                break;
            }
        }

        // Filter only files (not folders) and extract paths
        let file_paths: Vec<String> = all_entries
            .into_iter()
            .filter(|entry| entry.tag == "file")
            .filter_map(|entry| entry.path_display)
            .collect();

        Ok(Box::pin(stream::iter(file_paths.into_iter().map(Ok))))
    }
}
