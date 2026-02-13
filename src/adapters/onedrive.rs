use crate::{Error, Result, Storage};
use futures::stream::{BoxStream, StreamExt};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use std::fmt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// OneDrive adapter implementing [`Storage`] using **native OneDrive item IDs**.
///
/// # Identifier
/// `Id = String` is the OneDrive item ID (e.g. `12345abcd!123`).
///
/// # Auth
/// Requires a valid OAuth2 access token with `Files.ReadWrite` (or similar) scope.
#[derive(Clone)]
pub struct OneDriveStorage {
    client: Client,
    base_url: Url,
    token_provider: TokenProvider,
}

impl fmt::Debug for OneDriveStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneDriveStorage")
            .field("base_url", &self.base_url.as_str())
            .finish_non_exhaustive()
    }
}

/// How this adapter obtains the OAuth2 access token.
#[derive(Clone)]
pub enum TokenProvider {
    /// A fixed bearer token.
    Static(String),
    /// A user-provided async token callback.
    Callback(std::sync::Arc<dyn Fn() -> TokenFuture + Send + Sync>),
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
            TokenProvider::Static(t) => Ok(t.clone()),
            TokenProvider::Callback(cb) => (cb)().await,
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
        _id: Self::Id,
        _input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        // Native IDs usually imply updating an existing resource.
        // PUT /me/drive/items/{item-id}/content
        //
        // NOTE: This implementation buffers the stream because reqwest::Body
        // from a reader is not trivial without 'static bounds or specific dependencies.
        // Ideally, we'd use a stream body.
        Err(Error::Generic(
            "OneDriveStorage::put not implemented yet (requires streaming body support)"
                .to_string(),
        ))
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
