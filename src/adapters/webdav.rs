use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream, StreamExt};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// WebDAV storage adapter using HTTP Basic Authentication.
///
/// Supported by Nextcloud, ownCloud, and other WebDAV-compatible services.
/// Paths should not start with "/" (e.g., `"folder/file.txt"`).
#[derive(Clone, Debug)]
pub struct WebDAVStorage {
    client: Client,
    base_url: String,
    username: String,
    password: SecretString,
}

impl WebDAVStorage {
    /// Create a new WebDAV storage adapter.
    ///
    /// # Arguments
    /// - `base_url`: The WebDAV endpoint URL (e.g., "https://cloud.example.com/remote.php/dav/files/username")
    /// - `username`: Username for authentication
    /// - `password`: Password for authentication
    pub fn new(
        base_url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();

        Self {
            client: Client::new(),
            base_url,
            username: username.into(),
            password: SecretString::from(password.into()),
        }
    }

    fn resource_url(&self, path: &str) -> String {
        let clean_path = path.trim_start_matches('/');
        format!("{}/{}", self.base_url, clean_path)
    }

    fn map_error(&self, status: StatusCode, path: &str) -> Error {
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(path.to_string()),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Error::PermissionDenied("WebDAV authentication failed".to_string())
            }
            StatusCode::CONFLICT => {
                Error::Generic(format!("WebDAV conflict: parent directory may not exist"))
            }
            StatusCode::LOCKED => Error::PermissionDenied("Resource is locked".to_string()),
            StatusCode::INSUFFICIENT_STORAGE => {
                Error::Generic("Insufficient storage space".to_string())
            }
            _ => Error::Generic(format!("WebDAV error: {}", status)),
        }
    }

    async fn ensure_parent_dir(&self, path: &str) -> Result<()> {
        // Extract parent directory from path
        let path_parts: Vec<&str> = path.split('/').collect();
        if path_parts.len() <= 1 {
            return Ok(()); // No parent directory needed
        }

        // Build parent path
        let parent = &path_parts[..path_parts.len() - 1].join("/");
        if parent.is_empty() {
            return Ok(());
        }

        // Try to create parent directory (MKCOL)
        let parent_url = self.resource_url(parent);

        let response = self
            .client
            .request(reqwest::Method::from_bytes(b"MKCOL").unwrap(), &parent_url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        // 201 Created or 405 Method Not Allowed (already exists) are both OK
        match response.status() {
            StatusCode::CREATED | StatusCode::METHOD_NOT_ALLOWED | StatusCode::CONFLICT => Ok(()),
            _ => Ok(()), // Ignore errors, the PUT will fail if directory creation was necessary
        }
    }
}

impl Storage for WebDAVStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.resource_url(id);

        let response = self
            .client
            .head(&url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        Ok(response.status().is_success())
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        let url = self.resource_url(id);

        // Use PROPFIND to check if it's a collection (directory)
        let response = self
            .client
            .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), &url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .header("Depth", "0")
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !response.status().is_success() {
            return Ok(false);
        }

        // Check if the response indicates it's a collection
        let body = response
            .text()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        Ok(body.contains("<d:collection/>") || body.contains("collection"))
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        // Ensure parent directory exists
        self.ensure_parent_dir(&id).await?;

        let url = self.resource_url(&id);

        // Read data into memory
        let mut data = Vec::new();
        tokio::io::copy(&mut input, &mut data).await?;

        let response = self
            .client
            .put(&url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(data)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(self.map_error(response.status(), &id))
        }
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let url = self.resource_url(id);

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(self.map_error(status, id));
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
        let url = self.resource_url(id);

        let response = self
            .client
            .delete(&url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        // Success or 404 are both OK (idempotent delete)
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(self.map_error(response.status(), id))
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let path = prefix.map(|p| p.as_str()).unwrap_or("");
        let url = self.resource_url(path);

        // PROPFIND request with depth infinity to list all files
        let propfind_body = r#"<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:resourcetype/>
    <D:getcontentlength/>
  </D:prop>
</D:propfind>"#;

        let response = self
            .client
            .request(reqwest::Method::from_bytes(b"PROPFIND").unwrap(), &url)
            .basic_auth(&self.username, Some(self.password.expose_secret()))
            .header("Depth", "infinity")
            .header(CONTENT_TYPE, "application/xml")
            .body(propfind_body)
            .send()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(self.map_error(status, path));
        }

        let body = response
            .text()
            .await
            .map_err(|e| Error::Connection(Box::new(e)))?;

        // Parse XML response
        let file_paths = self.parse_propfind_response(&body, path)?;

        Ok(Box::pin(stream::iter(file_paths.into_iter().map(Ok))))
    }
}

impl WebDAVStorage {
    /// Parse WebDAV PROPFIND XML response to extract file paths.
    /// This is a simple parser - for production, consider using a proper XML library.
    fn parse_propfind_response(&self, xml: &str, prefix: &str) -> Result<Vec<String>> {
        let mut file_paths = Vec::new();
        let base_url_decoded = urlencoding::decode(&self.base_url)
            .unwrap_or_else(|_| std::borrow::Cow::Borrowed(&self.base_url));

        // Parse <D:response> blocks
        let responses: Vec<&str> = xml.split("<D:response>").skip(1).collect();

        for response_block in responses {
            // Check if it's a file (not a collection/directory)
            let is_collection = response_block.contains("<D:collection/>");
            if is_collection {
                continue;
            }

            // Extract href
            if let Some(href_start) = response_block.find("<D:href>") {
                if let Some(href_end) = response_block[href_start..].find("</D:href>") {
                    let href = &response_block[href_start + 8..href_start + href_end];

                    // Decode URL encoding
                    let decoded = urlencoding::decode(href)
                        .unwrap_or_else(|_| std::borrow::Cow::Borrowed(href));

                    // Remove base URL to get relative path
                    let relative_path =
                        if let Some(stripped) = decoded.strip_prefix(base_url_decoded.as_ref()) {
                            stripped.trim_start_matches('/')
                        } else if let Some(stripped) = decoded.strip_prefix(&self.base_url) {
                            stripped.trim_start_matches('/')
                        } else {
                            // Try to extract just the filename
                            decoded.trim_start_matches('/')
                        };

                    if !relative_path.is_empty() {
                        // Filter by prefix if specified
                        if prefix.is_empty() || relative_path.starts_with(prefix) {
                            file_paths.push(relative_path.to_string());
                        }
                    }
                }
            }
        }

        file_paths.sort();
        Ok(file_paths)
    }
}
