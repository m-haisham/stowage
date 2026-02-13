use crate::{Error, Result, Storage};
use futures::stream::{self, BoxStream};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// A local filesystem adapter.
///
/// - `Id` is a `String` representing a *relative* object path (e.g. `"foo/bar.txt"`).
/// - All objects are stored under a configured root directory.
/// - Paths are validated to prevent directory traversal (`..`) and absolute paths.
///
/// This implementation is intended to work with a `Storage` trait that uses Tokio I/O:
/// - `put` reads from a `tokio::io::AsyncRead`
/// - `get_into` writes into a `tokio::io::AsyncWrite`
///
/// If your current `Storage` trait still uses `futures::io::AsyncRead`, you'll need to
/// refactor it (as discussed) for this adapter to compile.
#[derive(Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

impl fmt::Debug for LocalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalStorage")
            .field("root", &self.root)
            .finish()
    }
}

impl LocalStorage {
    /// Create a new local storage rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Return the configured root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn validate_id(id: &str) -> Result<()> {
        if id.is_empty() {
            return Err(Error::Generic("id cannot be empty".into()));
        }

        let p = Path::new(id);

        if p.is_absolute() {
            return Err(Error::PermissionDenied(format!(
                "absolute paths are not allowed: {id}"
            )));
        }

        // Disallow traversal and tricky prefixes.
        for c in p.components() {
            match c {
                Component::ParentDir => {
                    return Err(Error::PermissionDenied(format!(
                        "parent dir components ('..') are not allowed: {id}"
                    )));
                }
                Component::Prefix(_) => {
                    // Windows drive prefixes like C:\
                    return Err(Error::PermissionDenied(format!(
                        "path prefixes are not allowed: {id}"
                    )));
                }
                Component::RootDir => {
                    return Err(Error::PermissionDenied(format!(
                        "root dir component is not allowed: {id}"
                    )));
                }
                Component::CurDir | Component::Normal(_) => {}
            }
        }

        Ok(())
    }

    fn path_for_id(&self, id: &str) -> Result<PathBuf> {
        Self::validate_id(id)?;
        Ok(self.root.join(id))
    }

    fn id_for_path(&self, p: &Path) -> Result<String> {
        let rel = p
            .strip_prefix(&self.root)
            .map_err(|e| Error::Generic(format!("failed to relativize path: {e}")))?;

        let s = rel
            .to_str()
            .ok_or_else(|| Error::Generic("non-utf8 path under root".into()))?
            .replace('\\', "/");

        Self::validate_id(&s)?;
        Ok(s)
    }

    async fn ensure_parent_dir(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    async fn list_recursive(&self, base: PathBuf) -> Result<Vec<String>> {
        // If the base doesn't exist, return empty list.
        let md = match tokio::fs::metadata(&base).await {
            Ok(md) => md,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };

        let mut out = Vec::new();

        if md.is_file() {
            out.push(self.id_for_path(&base)?);
            return Ok(out);
        }

        let mut stack = vec![base];
        while let Some(dir) = stack.pop() {
            let mut rd = tokio::fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                let ty = entry.file_type().await?;
                if ty.is_dir() {
                    stack.push(path);
                } else if ty.is_file() {
                    out.push(self.id_for_path(&path)?);
                }
            }
        }

        out.sort();
        Ok(out)
    }
}

impl Storage for LocalStorage {
    type Id = String;

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let path = self.path_for_id(id)?;
        match tokio::fs::metadata(path).await {
            Ok(md) => Ok(md.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn put<R: AsyncRead + Send + Unpin>(
        &self,
        id: Self::Id,
        mut input: R,
        _len: Option<u64>,
    ) -> Result<()> {
        let path = self.path_for_id(&id)?;
        Self::ensure_parent_dir(&path).await?;

        // Write to a temp file then rename into place for a more atomic update.
        let tmp_path = path.with_extension("tmp.stowage");
        let mut file = tokio::fs::File::create(&tmp_path).await?;

        // Stream copy.
        tokio::io::copy(&mut input, &mut file).await?;
        file.flush().await?;
        drop(file);

        // Best-effort replace.
        if tokio::fs::metadata(&path).await.is_ok() {
            let _ = tokio::fs::remove_file(&path).await;
        }
        tokio::fs::rename(&tmp_path, &path).await?;

        Ok(())
    }

    async fn get_into<W: AsyncWrite + Send + Unpin>(
        &self,
        id: &Self::Id,
        mut output: W,
    ) -> Result<u64> {
        let path = self.path_for_id(id)?;
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(id.clone()));
            }
            Err(e) => return Err(e.into()),
        };

        let n = tokio::io::copy(&mut file, &mut output).await?;
        output.flush().await?;
        Ok(n)
    }

    async fn delete(&self, id: &Self::Id) -> Result<()> {
        let path = self.path_for_id(id)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: Option<&Self::Id>) -> Result<BoxStream<'_, Result<Self::Id>>> {
        let base = match prefix {
            Some(p) => self.path_for_id(p)?,
            None => self.root.clone(),
        };

        // Eagerly list then stream results.
        // For huge trees, consider switching to an async-stream implementation.
        let ids = self.list_recursive(base).await?;
        Ok(Box::pin(stream::iter(ids.into_iter().map(Ok))))
    }
}
