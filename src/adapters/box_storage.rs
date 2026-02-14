use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream};
use reqwest::header::AUTHORIZATION;
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Box.com storage adapter using OAuth2 access tokens.
///
/// Uses numeric file IDs. For `put` operations, the `id` parameter is used as the filename.
/// Uploads default to root folder (ID "0"), customizable via `with_folder()`.
#[derive(Clone, Debug)]
pub struct BoxStorage {
    client: Client,
    access_token: SecretString,
    /// Parent folder ID for new uploads (default "0" = root)
    parent_folder_id: String,
}

impl BoxStorage {
    const API_URL: &'static str = "https://api.box.com/2.0";
    const UPLOAD_URL: &'static str = "https://upload.box.com/api/2.0";

    /// Create a new Box storage adapter with an access token.
    /// New files will be uploaded to the root folder (ID "0").
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            access_token: SecretString::from(access_token.into()),
            parent_folder_id: "0".to_string(),
        }
    }

    /// Create a new Box storage adapter with a specific parent folder for uploads.
    pub fn with_folder(access_token: impl Into<String>, folder_id: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            access_token: SecretString::from(access_token.into()),
            parent_folder_id: folder_id.into(),
        }
    }

    /// Get the parent folder ID used for new uploads.
    pub fn parent_folder_id(&self) -> &str {
        &self.parent_folder_id
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.access_token.expose_secret())
    }

    fn map_error(&self, status: StatusCode, context: &str, body: &str) -> Error {
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(context.to_string()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::PermissionDenied(format!("Box auth failed: {}", body))
            }
            StatusCode::CONFLICT => Error::Generic(format!("Box conflict: {}", body)),
            StatusCode::TOO_MANY_REQUESTS => Error::Generic("Box rate limit exceeded".to_string()),
            StatusCode::INSUFFICIENT_STORAGE => {
                Error::Generic("Box storage quota exceeded".to_string())
            }
            _ => Error::Generic(format!("Box error {}: {}", status, body)),
        }
    }

    /// Search for a file by name in the configured folder.
    /// Returns the file ID if found, None otherwise.
    async fn search_file_in_folder(&self, name: &str) -> Result<Option<String>> {
        let url = format!("{}/folders/{}/items", Self::API_URL, self.parent_folder_id);

        let response = self
            .client
            .get(&url)
            .header(AUTHORIZATION, self.auth_header())
            .query(&[("limit", "1000"), ("fields", "id,type,name")])
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.map_error(status, &self.parent_folder_id, &body));
        }

        let items: BoxFolderItems = response
            .json()
            .await
            .map_err(|e| Error::Generic(format!("failed to parse Box response: {e}")))?;

        // Find file with matching name
        Ok(items
            .entries
            .into_iter()
            .find(|item| item.item_type == "file" && item.name == name)
            .map(|item| item.id))
    }

    /// Create a new file in the configured folder.
    async fn create_file(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let url = format!("{}/files/content", Self::UPLOAD_URL);

        // Box upload API uses multipart/form-data
        let form = reqwest::multipart::Form::new()
            .part(
                "attributes",
                reqwest::multipart::Part::text(format!(
                    r#"{{"name":"{}","parent":{{"id":"{}"}}}}"#,
                    name, self.parent_folder_id
                ))
                .mime_str("application/json")
                .map_err(|e| Error::Generic(format!("invalid mime type: {e}")))?,
            )
            .part(
                "file",
                reqwest::multipart::Part::bytes(data)
                    .file_name(name.to_string())
                    .mime_str("application/octet-stream")
                    .map_err(|e| Error::Generic(format!("invalid mime type: {e}")))?,
            );

        let response = self
            .client
            .post(&url)
            .header(AUTHORIZATION, self.auth_header())
            .multipart(form)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match response.status() {
            StatusCode::CREATED => Ok(()),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, name, &body))
            }
        }
    }

    /// Update an existing file by ID.
    async fn update_file(&self, file_id: &str, data: Vec<u8>) -> Result<()> {
        let url = format!("{}/files/{}/content", Self::UPLOAD_URL, file_id);

        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(data)
                .mime_str("application/octet-stream")
                .map_err(|e| Error::Generic(format!("invalid mime type: {e}")))?,
        );

        let response = self
            .client
            .post(&url)
            .header(AUTHORIZATION, self.auth_header())
            .multipart(form)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(()),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, file_id, &body))
            }
        }
    }
}

#[derive(Deserialize)]
struct BoxFolderItems {
    entries: Vec<BoxFolderEntry>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    limit: u64,
    #[serde(default)]
    total_count: u64,
}

#[derive(Deserialize)]
struct BoxFolderEntry {
    id: String,
    #[serde(rename = "type")]
    item_type: String,
    name: String,
}

impl Storage for BoxStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let url = format!("{}/files/{}", Self::API_URL, id);

        let response = self
            .client
            .get(&url)
            .header(AUTHORIZATION, self.auth_header())
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, id, &body))
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        // Read the entire content into memory
        // Box API requires multipart/form-data upload
        let mut data = Vec::new();
        input
            .read_to_end(&mut data)
            .await
            .map_err(|e| Error::Generic(format!("failed to read data: {e}")))?;

        // First, try to get file info by searching in folder
        let search_result = self.search_file_in_folder(&id).await?;

        if let Some(file_id) = search_result {
            // File exists, update it
            self.update_file(&file_id, data).await
        } else {
            // File doesn't exist, create new
            self.create_file(&id, data).await
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let url = format!("{}/files/{}/content", Self::API_URL, id);

        let response = self
            .client
            .get(&url)
            .header(AUTHORIZATION, self.auth_header())
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.map_error(status, id, &body));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let total_bytes = bytes.len() as u64;

        output
            .write_all(&bytes)
            .await
            .map_err(|e| Error::Generic(format!("failed to write data: {e}")))?;

        output
            .flush()
            .await
            .map_err(|e| Error::Generic(format!("failed to flush writer: {e}")))?;

        Ok(total_bytes)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let url = format!("{}/files/{}", Self::API_URL, id);

        let response = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, self.auth_header())
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::NotFound(id.to_string())),
            status => {
                let body = response.text().await.unwrap_or_default();
                Err(self.map_error(status, id, &body))
            }
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        // Box doesn't have a traditional "prefix" concept since IDs are not hierarchical
        // We'll list all files in the configured folder
        // If prefix is provided, we'll treat it as a folder ID

        let folder_id = prefix.unwrap_or(&self.parent_folder_id);
        let url = format!("{}/folders/{}/items", Self::API_URL, folder_id);

        let response = self
            .client
            .get(&url)
            .header(AUTHORIZATION, self.auth_header())
            .query(&[("limit", "1000"), ("fields", "id,type,name")])
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(self.map_error(status, folder_id, &body));
        }

        let items: BoxFolderItems = response
            .json()
            .await
            .map_err(|e| Error::Generic(format!("failed to parse Box response: {e}")))?;

        // Filter to only return file IDs (not folders)
        let file_ids: Vec<Result<String>> = items
            .entries
            .into_iter()
            .filter(|item| item.item_type == "file")
            .map(|item| Ok(item.id))
            .collect();

        Ok(Box::pin(stream::iter(file_ids)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_box_storage_creation() {
        let storage = BoxStorage::new("test_token");
        assert_eq!(storage.parent_folder_id(), "0");
    }

    #[test]
    fn test_box_storage_with_folder() {
        let storage = BoxStorage::with_folder("test_token", "12345");
        assert_eq!(storage.parent_folder_id(), "12345");
    }
}
