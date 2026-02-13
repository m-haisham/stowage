use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream, StreamExt};
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// Azure Blob Storage adapter implementing [`Storage`].
///
/// # Identifier
/// - `Id` is a `String` representing the blob name (e.g., `"folder/file.txt"`).
/// - Blobs are stored in a single container.
///
/// # Authentication
/// This adapter uses a **Shared Access Signature (SAS)** token for authentication.
/// You can also use account key authentication by constructing the proper Authorization header.
///
/// # Example
/// ```ignore
/// use stowage::AzureStorage;
///
/// let storage = AzureStorage::new(
///     "mystorageaccount",
///     "mycontainer",
///     "sp=racwdl&st=2024-01-01T00:00:00Z&se=2024-12-31T23:59:59Z&spr=https&sv=2021-06-08&sr=c&sig=..."
/// );
/// ```
#[derive(Clone, Debug)]
pub struct AzureStorage {
    client: Client,
    account: String,
    container: String,
    sas_token: SecretString,
    base_url: String,
}

impl AzureStorage {
    /// Create a new Azure Blob Storage adapter.
    ///
    /// # Arguments
    /// - `account`: Storage account name (e.g., "mystorageaccount")
    /// - `container`: Container name (e.g., "mycontainer")
    /// - `sas_token`: Shared Access Signature token (without leading '?')
    pub fn new(
        account: impl Into<String>,
        container: impl Into<String>,
        sas_token: impl Into<String>,
    ) -> Self {
        let account = account.into();
        let container = container.into();
        let base_url = format!("https://{}.blob.core.windows.net/{}", account, container);

        Self {
            client: Client::new(),
            account,
            container,
            sas_token: SecretString::from(sas_token.into()),
            base_url,
        }
    }

    /// Create a new Azure Blob Storage adapter with custom endpoint (for emulators).
    pub fn with_endpoint(
        account: impl Into<String>,
        container: impl Into<String>,
        sas_token: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        let account = account.into();
        let container = container.into();
        let endpoint = endpoint.into();
        let base_url = format!("{}/{}", endpoint.trim_end_matches('/'), container);

        Self {
            client: Client::new(),
            account,
            container,
            sas_token: SecretString::from(sas_token.into()),
            base_url,
        }
    }

    fn blob_url(&self, blob_name: &str) -> String {
        format!(
            "{}/{}?{}",
            self.base_url,
            blob_name,
            self.sas_token.expose_secret()
        )
    }

    fn map_status_error(&self, status: StatusCode, blob_name: &str) -> Error {
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(blob_name.to_string()),
            StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => {
                Error::PermissionDenied("Azure authentication failed".to_string())
            }
            _ => Error::Generic(format!("Azure Blob Storage error: {}", status)),
        }
    }
}

impl Storage for AzureStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.blob_url(id);

        let response = self
            .client
            .head(&url)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        Ok(response.status().is_success())
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        len: Option<u64>,
    ) -> Result<()> {
        let url = self.blob_url(&id);

        // Read all data into memory
        let mut data = Vec::new();
        tokio::io::copy(&mut input, &mut data)
            .await
            .map_err(|e| Error::Io(e))?;

        let mut request = self
            .client
            .put(&url)
            .header("x-ms-blob-type", "BlockBlob")
            .body(data);

        if let Some(len) = len {
            request = request.header("Content-Length", len.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            return Err(self.map_status_error(response.status(), &id));
        }

        Ok(())
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let url = self.blob_url(id);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            return Err(self.map_status_error(response.status(), id));
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
        let url = self.blob_url(id);

        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        // 202 Accepted or 404 Not Found are both OK (idempotent delete)
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(self.map_status_error(response.status(), id))
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let prefix_str = prefix.map(|s| s.as_str()).unwrap_or("");

        // Build list blobs URL
        let mut url = format!(
            "{}?restype=container&comp=list&{}",
            self.base_url,
            self.sas_token.expose_secret()
        );
        if !prefix_str.is_empty() {
            url.push_str(&format!("&prefix={}", urlencoding::encode(prefix_str)));
        }

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if !response.status().is_success() {
            return Err(self.map_status_error(
                response.status(),
                &format!("list with prefix: {}", prefix_str),
            ));
        }

        let body = response
            .text()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        // Parse XML response
        let blob_names = self.parse_list_response(&body)?;

        Ok(Box::pin(stream::iter(blob_names.into_iter().map(Ok))))
    }
}

impl AzureStorage {
    /// Parse Azure Blob Storage XML list response.
    /// This is a simple parser - for production use, consider using a proper XML library.
    fn parse_list_response(&self, xml: &str) -> Result<Vec<String>> {
        let mut blob_names = Vec::new();

        // Simple XML parsing - look for <Name>...</Name> tags within <Blob> sections
        let mut in_blob = false;
        let mut capturing_name = false;
        let mut current_name = String::new();

        for line in xml.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("<Blob>") {
                in_blob = true;
            } else if trimmed.starts_with("</Blob>") {
                in_blob = false;
            } else if in_blob {
                if trimmed.starts_with("<Name>") {
                    capturing_name = true;
                    // Extract name between tags
                    if let Some(start) = trimmed.find("<Name>") {
                        if let Some(end) = trimmed.find("</Name>") {
                            let name = &trimmed[start + 6..end];
                            blob_names.push(name.to_string());
                            capturing_name = false;
                        } else {
                            current_name = trimmed[start + 6..].to_string();
                        }
                    }
                } else if capturing_name && trimmed.ends_with("</Name>") {
                    if let Some(end) = trimmed.find("</Name>") {
                        current_name.push_str(&trimmed[..end]);
                        blob_names.push(current_name.clone());
                        current_name.clear();
                        capturing_name = false;
                    }
                } else if capturing_name {
                    current_name.push_str(trimmed);
                }
            }
        }

        Ok(blob_names)
    }
}
