use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream};
use secrecy::{ExposeSecret, SecretString};
use std::path::PathBuf;
use std::sync::Arc;
use suppaftp::AsyncFtpStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

/// FTP storage adapter using suppaftp.
///
/// Supports username/password authentication.
pub struct FtpStorage {
    host: String,
    port: u16,
    username: String,
    password: SecretString,
    base_path: Option<PathBuf>,
    // FTP connection wrapped in Arc<Mutex> for thread-safe access
    // In production, consider connection pooling
    stream: Arc<Mutex<AsyncFtpStream>>,
}

impl std::fmt::Debug for FtpStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpStorage")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("base_path", &self.base_path)
            .finish()
    }
}

impl FtpStorage {
    /// Create a new FTP storage adapter.
    /// - `address`: The FTP server address (e.g., "ftp.example.com:21" or "192.168.1.1:21")
    /// - `username`: Username for authentication
    /// - `password`: Password for authentication
    /// - `base_path`: Optional base path to prefix all file operations
    pub async fn new(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        base_path: Option<PathBuf>,
    ) -> Result<Self> {
        let address = address.into();
        let username = username.into();
        let password = SecretString::from(password.into());

        // Parse host and port
        let (host, port) = if let Some((h, p)) = address.split_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|e| Error::Generic(format!("Invalid port: {}", e)))?;
            (h.to_string(), port)
        } else {
            (address, 21)
        };

        // Establish FTP connection
        let mut stream = AsyncFtpStream::connect(format!("{}:{}", host, port))
            .await
            .map_err(|e| {
                Error::Connection(Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!("FTP connection failed: {}", e),
                )))
            })?;

        // Login
        let password_str = password.expose_secret().to_string();
        stream
            .login(&username, &password_str)
            .await
            .map_err(|e| Error::PermissionDenied(format!("FTP authentication failed: {}", e)))?;

        // Set binary mode for file transfers
        stream
            .transfer_type(suppaftp::types::FileType::Binary)
            .await
            .map_err(|e| Error::Generic(format!("Failed to set binary mode: {}", e)))?;

        // Change to base directory if specified
        if let Some(ref base) = base_path {
            let base_str = base.to_string_lossy();
            if !base_str.is_empty() {
                stream.cwd(&base_str).await.map_err(|e| {
                    Error::Generic(format!("Failed to change to base directory: {}", e))
                })?;
            }
        }

        Ok(Self {
            host,
            port,
            username,
            password,
            base_path,
            stream: Arc::new(Mutex::new(stream)),
        })
    }

    /// Get the full path by combining base_path with the given path
    fn full_path(&self, path: &str) -> String {
        if let Some(base) = &self.base_path {
            base.join(path).to_string_lossy().to_string()
        } else {
            path.to_string()
        }
    }

    /// Check if error is "not found"
    fn is_not_found_error(error_msg: &str) -> bool {
        error_msg.contains("550") || error_msg.to_lowercase().contains("not found")
    }

    /// Ensure parent directories exist
    async fn ensure_parent_dir(&self, path: &str) -> Result<()> {
        let path_obj = std::path::Path::new(path);
        if let Some(parent) = path_obj.parent() {
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent_str != "/" {
                let mut stream = self.stream.lock().await;

                // Try to create parent directories recursively
                let mut current = PathBuf::new();
                for component in parent.components() {
                    current.push(component);
                    let current_str = current.to_string_lossy();
                    if !current_str.is_empty() {
                        // Try to create directory, ignore errors if it already exists
                        let _ = stream.mkdir(&current_str).await;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Storage for FtpStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let path = self.full_path(id);
        let mut stream = self.stream.lock().await;

        match stream.size(&path).await {
            Ok(_) => Ok(true),
            Err(e) => {
                let error_msg = e.to_string();
                if Self::is_not_found_error(&error_msg) {
                    Ok(false)
                } else {
                    Err(Error::Generic(format!("Failed to check file: {}", e)))
                }
            }
        }
    }

    async fn folder_exists(&self, id: &Self::Id) -> Result<bool> {
        let path = self.full_path(id);
        let mut stream = self.stream.lock().await;

        // Try to change to the directory
        match stream.cwd(&path).await {
            Ok(_) => Ok(true),
            Err(e) => {
                let error_msg = e.to_string();
                if Self::is_not_found_error(&error_msg) {
                    Ok(false)
                } else {
                    Err(Error::Generic(format!("Failed to check folder: {}", e)))
                }
            }
        }
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        let path = self.full_path(&id);

        // Ensure parent directory exists
        self.ensure_parent_dir(&path).await?;

        // Read all data into memory
        let mut buffer = Vec::new();
        input
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| Error::Io(e))?;

        let mut stream = self.stream.lock().await;

        // Upload the file - suppaftp expects futures::io::AsyncRead
        use futures::io::AllowStdIo;
        let mut cursor = AllowStdIo::new(std::io::Cursor::new(buffer));

        stream
            .put_file(&path, &mut cursor)
            .await
            .map_err(|e| Error::Generic(format!("Failed to upload file: {}", e)))?;

        Ok(())
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let path = self.full_path(id);
        let mut stream = self.stream.lock().await;

        // Retrieve the file into a buffer using retr method
        let buffer: Vec<u8> = stream
            .retr(&path, |mut reader| {
                Box::pin(async move {
                    use futures::io::AsyncReadExt;
                    let mut temp_buf = Vec::new();
                    reader
                        .read_to_end(&mut temp_buf)
                        .await
                        .map_err(suppaftp::FtpError::ConnectionError)?;
                    Ok((temp_buf, reader))
                })
            })
            .await
            .map_err(|e| {
                let error_msg = e.to_string();
                if Self::is_not_found_error(&error_msg) {
                    Error::NotFound(id.clone())
                } else {
                    Error::Generic(format!("Failed to download file: {}", e))
                }
            })?;

        let len = buffer.len() as u64;

        output.write_all(&buffer).await.map_err(|e| Error::Io(e))?;
        output.flush().await.map_err(|e| Error::Io(e))?;

        Ok(len)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let path = self.full_path(id);
        let mut stream = self.stream.lock().await;

        match stream.rm(&path).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let error_msg = e.to_string();
                if Self::is_not_found_error(&error_msg) {
                    // File doesn't exist - idempotent delete
                    Ok(())
                } else {
                    Err(Error::Generic(format!("Failed to delete file: {}", e)))
                }
            }
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let dir_path = if let Some(p) = prefix {
            self.full_path(p)
        } else {
            ".".to_string()
        };

        let mut stream = self.stream.lock().await;

        // List files in directory
        let entries = match stream.list(Some(&dir_path)).await {
            Ok(e) => e,
            Err(e) => {
                let error_msg = e.to_string();
                if Self::is_not_found_error(&error_msg) {
                    // Directory doesn't exist, return empty list
                    return Ok(Box::pin(stream::iter(Vec::new().into_iter().map(Ok))));
                } else {
                    return Err(Error::Generic(format!("Failed to list directory: {}", e)));
                }
            }
        };

        // Parse file names from LIST output
        let mut results = Vec::new();
        for entry in entries {
            // FTP LIST format varies by server, but typically:
            // "-rw-r--r--   1 user  group      1234 Jan 01 12:00 filename.txt"
            // We'll parse the last field as the filename
            if let Some(filename) = entry.split_whitespace().last() {
                if filename != "." && filename != ".." {
                    let full_name = if dir_path == "." || dir_path.is_empty() {
                        filename.to_string()
                    } else {
                        format!("{}/{}", dir_path.trim_end_matches('/'), filename)
                    };

                    // Apply prefix filter if specified
                    if let Some(ref prefix) = prefix {
                        if full_name.starts_with(*prefix) {
                            results.push(full_name);
                        }
                    } else {
                        results.push(full_name);
                    }
                }
            }
        }

        Ok(Box::pin(stream::iter(results.into_iter().map(Ok))))
    }
}
