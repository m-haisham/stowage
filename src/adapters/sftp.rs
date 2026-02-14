use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream};
use secrecy::{ExposeSecret, SecretString};
use ssh2::{Session, Sftp};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// SFTP storage adapter using SSH2 for secure file transfers.
///
/// Supports password-based authentication.
pub struct SftpStorage {
    host: String,
    port: u16,
    username: String,
    password: SecretString,
    base_path: Option<PathBuf>,
    // SSH2 Session is not thread-safe, so we wrap in Arc<Mutex>
    // In production, consider connection pooling
    session: Arc<Mutex<Session>>,
}

impl std::fmt::Debug for SftpStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpStorage")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("base_path", &self.base_path)
            .finish()
    }
}

impl SftpStorage {
    /// Create a new SFTP storage adapter.
    /// - `address`: The SFTP server address (e.g., "sftp.example.com:22" or "192.168.1.1:22")
    /// - `username`: Username for authentication
    /// - `password`: Password for authentication
    /// - `base_path`: Optional base path to prefix all file operations
    pub async fn new(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
        base_path: Option<impl Into<PathBuf>>,
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
            (address, 22)
        };

        // Establish SSH connection
        let session = tokio::task::spawn_blocking({
            let host = host.clone();
            let username = username.clone();
            let password = password.clone();
            move || -> Result<Session> {
                let tcp = TcpStream::connect(format!("{}:{}", host, port))
                    .map_err(|e| Error::Connection(Box::new(e)))?;

                let mut session = Session::new().map_err(|e| Error::Connection(Box::new(e)))?;
                session.set_tcp_stream(tcp);
                session
                    .handshake()
                    .map_err(|e| Error::Connection(Box::new(e)))?;
                session
                    .userauth_password(&username, password.expose_secret())
                    .map_err(|e| Error::PermissionDenied(format!("SFTP auth failed: {}", e)))?;

                if !session.authenticated() {
                    return Err(Error::PermissionDenied(
                        "SFTP authentication failed".to_string(),
                    ));
                }

                Ok(session)
            }
        })
        .await
        .map_err(|e| Error::Generic(format!("Task join error: {}", e)))??;

        Ok(Self {
            host,
            port,
            username,
            password,
            base_path: base_path.map(|p| p.into()),
            session: Arc::new(Mutex::new(session)),
        })
    }

    /// Get the full path by combining base_path with the given path
    fn full_path(&self, path: &str) -> PathBuf {
        if let Some(base) = &self.base_path {
            base.join(path)
        } else {
            PathBuf::from(path)
        }
    }

    /// Execute an SFTP operation in a blocking task
    fn with_sftp<F, R>(&self, f: F) -> impl std::future::Future<Output = Result<R>> + Send
    where
        F: FnOnce(&Sftp) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let session = Arc::clone(&self.session);
        async move {
            tokio::task::spawn_blocking(move || {
                let session = session
                    .lock()
                    .map_err(|e| Error::Generic(format!("Mutex lock failed: {}", e)))?;
                let sftp = session
                    .sftp()
                    .map_err(|e| Error::Generic(format!("SFTP channel failed: {}", e)))?;
                f(&sftp)
            })
            .await
            .map_err(|e| Error::Generic(format!("Task join error: {}", e)))?
        }
    }

    /// Ensure parent directories exist
    fn ensure_parent_dir(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            let session = self
                .session
                .lock()
                .map_err(|e| Error::Generic(format!("Mutex lock failed: {}", e)))?;
            let sftp = session
                .sftp()
                .map_err(|e| Error::Generic(format!("SFTP channel failed: {}", e)))?;

            // Try to create parent directories recursively
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent_str != "/" {
                // Try to stat the parent; if it doesn't exist, try to create it
                if sftp.stat(parent).is_err() {
                    // Create parent recursively
                    let mut current = PathBuf::new();
                    for component in parent.components() {
                        current.push(component);
                        if sftp.stat(&current).is_err() {
                            sftp.mkdir(&current, 0o755).ok(); // Ignore errors for existing dirs
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Storage for SftpStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let path = self.full_path(id);
        self.with_sftp(move |sftp| {
            match sftp.stat(&path) {
                Ok(_) => Ok(true),
                Err(e) => {
                    // Check if it's a "not found" error
                    let error_msg = e.to_string();
                    if error_msg.contains("no such file")
                        || error_msg.contains("LIBSSH2_FX_NO_SUCH_FILE")
                    {
                        Ok(false)
                    } else {
                        Err(Error::Generic(format!("SFTP stat failed: {}", e)))
                    }
                }
            }
        })
        .await
    }

    async fn put<R: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        let path = self.full_path(&id);

        // Ensure parent directory exists
        self.ensure_parent_dir(&path)?;

        // Read all data into memory (for now - could be improved with streaming)
        let mut buffer = Vec::new();
        tokio::io::copy(&mut input, &mut buffer)
            .await
            .map_err(|e| Error::Io(e))?;

        self.with_sftp(move |sftp| {
            let mut remote_file = sftp
                .create(&path)
                .map_err(|e| Error::Generic(format!("SFTP create failed: {}", e)))?;

            remote_file.write_all(&buffer).map_err(|e| Error::Io(e))?;

            remote_file.flush().map_err(|e| Error::Io(e))?;

            Ok(())
        })
        .await
    }

    async fn get_into<W: AsyncWrite + Send + Sync + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let path = self.full_path(id);
        let id_clone = id.clone();

        let buffer = self
            .with_sftp(move |sftp| {
                let mut remote_file = sftp.open(&path).map_err(|e| {
                    let error_msg = e.to_string();
                    if error_msg.contains("no such file")
                        || error_msg.contains("LIBSSH2_FX_NO_SUCH_FILE")
                    {
                        Error::NotFound(id_clone.clone())
                    } else {
                        Error::Generic(format!("SFTP open failed: {}", e))
                    }
                })?;

                let mut buffer = Vec::new();
                remote_file
                    .read_to_end(&mut buffer)
                    .map_err(|e| Error::Io(e))?;

                Ok(buffer)
            })
            .await?;

        let len = buffer.len() as u64;
        output.write_all(&buffer).await.map_err(|e| Error::Io(e))?;
        output.flush().await.map_err(|e| Error::Io(e))?;

        Ok(len)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let path = self.full_path(id);

        self.with_sftp(move |sftp| {
            match sftp.unlink(&path) {
                Ok(_) => Ok(()),
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("no such file")
                        || error_msg.contains("LIBSSH2_FX_NO_SUCH_FILE")
                    {
                        // File doesn't exist - idempotent delete
                        Ok(())
                    } else {
                        Err(Error::Generic(format!("SFTP delete failed: {}", e)))
                    }
                }
            }
        })
        .await
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let dir_path = if let Some(p) = prefix {
            self.full_path(p)
        } else if let Some(base) = &self.base_path {
            base.clone()
        } else {
            PathBuf::from(".")
        };

        let prefix_str = prefix.map(|s| s.to_string());
        let base_path = self.base_path.clone();

        let entries = self
            .with_sftp(move |sftp| {
                let mut results = Vec::new();

                // Try to read directory
                match sftp.readdir(&dir_path) {
                    Ok(entries) => {
                        for (path, stat) in entries {
                            // Only include regular files
                            if stat.is_file() {
                                // Convert path back to relative string
                                let path_str = if let Some(base) = &base_path {
                                    path.strip_prefix(base)
                                        .unwrap_or(&path)
                                        .to_string_lossy()
                                        .to_string()
                                } else {
                                    path.to_string_lossy().to_string()
                                };

                                // Apply prefix filter if specified
                                if let Some(ref prefix) = prefix_str {
                                    if path_str.starts_with(prefix) {
                                        results.push(path_str);
                                    }
                                } else {
                                    results.push(path_str);
                                }
                            }
                        }
                        Ok(results)
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("no such file")
                            || error_msg.contains("LIBSSH2_FX_NO_SUCH_FILE")
                        {
                            // Directory doesn't exist, return empty list
                            Ok(Vec::new())
                        } else {
                            Err(Error::Generic(format!("SFTP readdir failed: {}", e)))
                        }
                    }
                }
            })
            .await?;

        Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))))
    }
}
