use crate::{Error, Result, Storage};
use futures::stream::{BoxStream, StreamExt};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Google Drive adapter implementing [`Storage`] using **native Google Drive file IDs**.
///
/// ## Identifier
/// `Id = String` where the string is the Drive `fileId` (e.g. `"1ZdR3L...abc"`).
///
/// ## Auth
/// This adapter expects you to supply an OAuth2 access token (and refresh it externally).
///
/// ## Put Semantics
/// The `put` operation updates an existing file's content by its file ID.
/// To create new files, you would need to use the Google Drive API directly
/// to create the file and obtain its ID first.
///
/// ## Feature flags
/// Intended to be used behind the `gdrive` feature.
#[derive(Clone, Debug)]
pub struct GoogleDriveStorage {
    client: Client,
    base_url: Url,
    token_provider: TokenProvider,
}

/// How this adapter obtains the OAuth2 access token.
#[derive(Clone)]
pub enum TokenProvider {
    /// A fixed bearer token.
    Static(SecretString),
    /// A user-provided async token callback.
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
