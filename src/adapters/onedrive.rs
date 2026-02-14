use crate::{Error, Result, Storage};
use futures::stream::{BoxStream, StreamExt};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// OneDrive storage adapter using native item IDs.
///
/// Requires OAuth2 access token. The `put` operation updates existing files by ID.
#[derive(Clone, Debug)]
pub struct OneDriveStorage {
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

impl OneDriveStorage {
    /// Create a new `OneDriveStorage`.
    pub fn new(client: Client, token_provider: TokenProvider) -> Result<Self> {
        Ok(Self {
            client,
            base_url: Url::parse("https://graph.microsoft.com/v1.0/")
                .map_err(|e| Error::Generic(format!("invalid base url: {e}")))?,
            token_provider,
        })
    }

    async fn access_token(&self) -> Result<String> {
        match &self.token_provider {
            TokenProvider::Static(tok) => Ok(tok.expose_secret().to_string()),
            TokenProvider::Callback(f) => f().await,
        }
    }

    async fn auth_headers(&self) -> Result<HeaderMap> {
        let token = self.access_token().await?;
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| Error::Generic(format!("invalid bearer token header value: {e}")))?;
        headers.insert(AUTHORIZATION, value);
        Ok(headers)
    }

    fn item_url(&self, item_id: &str) -> Result<Url> {
        if item_id.is_empty() {
            return Err(Error::Generic(
                "onedrive item id cannot be empty".to_string(),
            ));
        }
        self.base_url
            .join(&format!("me/drive/items/{item_id}"))
            .map_err(|e| Error::Generic(format!("failed to build item url: {e}")))
    }

    fn content_url(&self, item_id: &str) -> Result<Url> {
        self.base_url
            .join(&format!("me/drive/items/{item_id}/content"))
            .map_err(|e| Error::Generic(format!("failed to build content url: {e}")))
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

impl Storage for OneDriveStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.item_url(id)?;
        let headers = self.auth_headers().await?;

        let resp = self
            .client
            .get(url)
            .headers(headers)
            .query(&[("select", "id")]) // Fetch minimal metadata
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        match resp.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let text = resp.text().await.unwrap_or_default();
                Err(Self::map_http_error(
                    status,
                    &text,
                    "onedrive exists failed",
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
        // Update existing file content by item ID
        // PUT /me/drive/items/{item-id}/content
        let url = self.content_url(&id)?;
        let headers = self.auth_headers().await?;

        // Read data into memory
        // OneDrive API works well with buffered uploads for small-medium files
        let mut data = Vec::new();
        tokio::io::copy(&mut input, &mut data)
            .await
            .map_err(|e| Error::Io(e))?;

        let mut request = self
            .client
            .put(url)
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
            Err(Self::map_http_error(status, &text, "onedrive put failed"))
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let url = self.content_url(id)?;
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
                "onedrive get_into failed",
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
        let url = self.item_url(id)?;
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
                Err(Self::map_http_error(
                    status,
                    &text,
                    "onedrive delete failed",
                ))
            }
        }
    }

    async fn list(&self, _prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        Err(Error::Generic(
            "OneDriveStorage::list not implemented yet (requires paging logic)".to_string(),
        ))
    }
}
