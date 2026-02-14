use crate::{Error, Result, Storage};
use futures::stream::{BoxStream, StreamExt};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Google Drive storage adapter using native file IDs.
///
/// Requires OAuth2 access token. The `put` operation updates existing files by ID.
#[derive(Clone, Debug)]
pub struct GoogleDriveStorage {
    client: Client,
    base_url: Url,
    token_provider: TokenProvider,
}

/// OAuth2 token provider.
#[derive(Clone)]
pub enum TokenProvider {
    /// Fixed bearer token.
    Static(SecretString),
    /// Async token callback.
    Callback(std::sync::Arc<dyn Fn() -> TokenFuture + Send + Sync>),
}

impl std::fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenProvider::Static(_) => f.debug_tuple("Static").field(&"<redacted>").finish(),
            TokenProvider::Callback(_) => f.debug_tuple("Callback").finish(),
        }
    }
}

type TokenFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send>>;

impl GoogleDriveStorage {
    /// Create a new `GoogleDriveStorage` given a reqwest client and token provider.
    pub fn new(client: Client, token_provider: TokenProvider) -> Result<Self> {
        Ok(Self {
            client,
            base_url: Url::parse("https://www.googleapis.com/drive/v3/")
                .map_err(|e| Error::Generic(format!("invalid base url: {e}")))?,
            token_provider,
        })
    }

    /// Override the base URL (useful for tests/mocks).
    pub fn with_base_url(mut self, base_url: Url) -> Self {
        self.base_url = base_url;
        self
    }

    async fn get_token(&self) -> Result<String> {
        match &self.token_provider {
            TokenProvider::Static(tok) => Ok(tok.expose_secret().to_string()),
            TokenProvider::Callback(f) => f().await,
        }
    }

    async fn auth_headers(&self) -> Result<HeaderMap> {
        let token = self.get_token().await?;
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| Error::Generic(format!("invalid bearer token header value: {e}")))?;
        headers.insert(AUTHORIZATION, value);
        Ok(headers)
    }

    fn file_url(&self, file_id: &str) -> Result<Url> {
        if file_id.is_empty() {
            return Err(Error::Generic("gdrive file id cannot be empty".to_string()));
        }
        self.base_url
            .join(&format!("files/{file_id}"))
            .map_err(|e| Error::Generic(format!("failed to build file url: {e}")))
    }

    fn download_url(&self, file_id: &str) -> Result<Url> {
        let mut url = self.file_url(file_id)?;
        url.query_pairs_mut().append_pair("alt", "media");
        Ok(url)
    }

    fn upload_url(&self, file_id: &str) -> Result<Url> {
        // Use the upload endpoint for updating file content
        let base = "https://www.googleapis.com/upload/drive/v3/";
        let url_str = format!("{}files/{}?uploadType=media", base, file_id);
        Url::parse(&url_str).map_err(|e| Error::Generic(format!("failed to build upload url: {e}")))
    }

    fn map_http_error(status: StatusCode, body_snippet: &str, context: &str) -> Error {
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(context.to_string()),
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                Error::PermissionDenied(format!("{context}: {status}"))
            }
            _ => Error::Generic(format!("{context}: {status} ({body_snippet})")),
        }
    }

    /// Find a folder by name in a specific parent folder.
    ///
    /// Returns the folder ID if found, or `None` if not found.
    /// If `parent_id` is `None`, searches in the root.
    ///
    /// Note: This performs a search and returns the first match.
    /// If multiple folders have the same name, only the first is returned.
    pub async fn find_folder_by_name(
        &self,
        name: &str,
        parent_id: Option<&str>,
    ) -> Result<Option<String>> {
        let headers = self.auth_headers().await?;

        // Build query: name matches and is a folder
        let mut query = format!(
            "name = '{}' and mimeType = 'application/vnd.google-apps.folder'",
            name.replace("'", "\\'")
        );

        if let Some(parent) = parent_id {
            query.push_str(&format!(" and '{}' in parents", parent.replace("'", "\\'")));
        }

        let resp = self
            .client
            .get(
                self.base_url
                    .join("files")
                    .map_err(|e| Error::Generic(format!("failed to build search url: {e}")))?,
            )
            .headers(headers)
            .query(&[("q", &query), ("fields", &"files(id)".to_string())])
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Self::map_http_error(status, &text, "gdrive search failed"));
        }

        let text = resp.text().await.unwrap_or_default();

        // Parse JSON to extract the first file ID
        if text.contains("\"files\"") {
            // Simple extraction - look for first "id" field
            if let Some(start) = text.find(r#""id":"#) {
                let after_id = &text[start + 6..];
                if let Some(end) = after_id.find('"') {
                    return Ok(Some(after_id[..end].to_string()));
                }
            }
        }

        Ok(None)
    }
}

impl Storage for GoogleDriveStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.file_url(id)?;
        let headers = self.auth_headers().await?;

        // Use a lightweight GET with `fields=id` to check existence.
        let resp = self
            .client
            .get(url)
            .headers(headers)
            .query(&[("fields", "id")])
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match resp.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(Self::map_http_error(status, &text, "gdrive exists failed"))
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.file_url(id)?;
        let headers = self.auth_headers().await?;

        // Check if the item exists and is a folder by checking mimeType
        let resp = self
            .client
            .get(url)
            .headers(headers)
            .query(&[("fields", "id,mimeType")])
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match resp.status() {
            StatusCode::OK => {
                let text = resp.text().await.unwrap_or_default();
                Ok(text.contains("application/vnd.google-apps.folder"))
            }
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(Self::map_http_error(
                    status,
                    &text,
                    "gdrive folder_exists failed",
                ))
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        len: Option<u64>,
    ) -> Result<()> {
        // Update existing file content by ID using the upload endpoint
        let url = self.upload_url(&id)?;
        let headers = self.auth_headers().await?;

        // Read data into memory
        // Google Drive API requires knowing content length for uploads
        let mut data = Vec::new();
        tokio::io::copy(&mut input, &mut data)
            .await
            .map_err(|e| Error::Io(e))?;

        let mut request = self
            .client
            .patch(url)
            .headers(headers)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(data);

        if let Some(len) = len {
            request = request.header("Content-Length", len.to_string());
        }

        let resp = request
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            Err(Self::map_http_error(status, &text, "gdrive put failed"))
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let url = self.download_url(id)?;
        let headers = self.auth_headers().await?;

        let resp = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            if status == StatusCode::NOT_FOUND {
                return Err(Error::NotFound(id.clone()));
            }
            return Err(Self::map_http_error(
                status,
                &text,
                "gdrive get_into failed",
            ));
        }

        let mut stream = resp.bytes_stream();
        let mut total = 0;
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(|e| Error::Connection(Box::new(e)))?;
            output.write_all(&bytes).await?;
            total += bytes.len() as u64;
        }
        output.flush().await?;

        Ok(total)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let url = self.file_url(id)?;
        let headers = self.auth_headers().await?;

        let resp = self
            .client
            .delete(url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match resp.status() {
            StatusCode::NO_CONTENT | StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Ok(()), // idempotent
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(Self::map_http_error(status, &text, "gdrive delete failed"))
            }
        }
    }

    async fn list(&self, _prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        Err(Error::Generic(
            "GoogleDriveStorage::list is not implemented yet.".to_string(),
        ))
    }
}
