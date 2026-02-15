use crate::{Error, Result, Storage};
use aws_sdk_s3::{Client, primitives::ByteStream};
use futures::stream::BoxStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// AWS S3 storage adapter using object keys as identifiers.
#[derive(Clone, Debug)]
pub struct S3Storage {
    client: Client,
    bucket: String,
}

impl S3Storage {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(Error::Generic("s3 key cannot be empty".to_string()));
        }
        Ok(())
    }

    fn map_sdk_err<E>(e: E) -> Error
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Connection(Box::new(e))
    }
}

impl Storage for S3Storage {
    type Id = String;

    fn exists(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<bool>> + Send {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = id.clone();

        async move {
            Self::validate_key(&key)?;

            let resp = client.head_object().bucket(bucket).key(key).send().await;

            match resp {
                Ok(_) => Ok(true),
                Err(e) => {
                    // `head_object` returns a modeled service error; treat 404/NoSuchKey as false.
                    // Check both the error string and the service error metadata
                    let msg = e.to_string();
                    let meta_str = format!("{:?}", e);
                    if msg.contains("NotFound")
                        || msg.contains("NoSuchKey")
                        || msg.contains("404")
                        || msg.contains("StatusCode(404)")
                        || meta_str.contains("NotFound")
                        || meta_str.contains("NoSuchKey")
                    {
                        Ok(false)
                    } else {
                        Err(Self::map_sdk_err(e))
                    }
                }
            }
        }
    }

    fn folder_exists(
        &self,
        id: &Self::Id,
    ) -> impl std::future::Future<Output = Result<bool>> + Send {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let mut prefix = id.clone();

        async move {
            Self::validate_key(&prefix)?;

            // In S3, folders don't exist as objects - they're just prefixes
            // Check if any objects exist with this prefix
            if !prefix.ends_with('/') {
                prefix.push('/');
            }

            let resp = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .max_keys(1)
                .send()
                .await
                .map_err(Self::map_sdk_err)?;

            Ok(resp.key_count().unwrap_or(0) > 0)
        }
    }

    fn put<I: AsyncRead + Send + Sync + Unpin>(
        &self,
        id: Self::Id,
        mut input: I,
        _len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = id;

        async move {
            Self::validate_key(&key)?;

            // Buffer the input (tokio AsyncRead) into memory.
            let mut buf = Vec::new();
            input.read_to_end(&mut buf).await?;

            let body = ByteStream::from(buf);

            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .map_err(Self::map_sdk_err)?;

            Ok(())
        }
    }

    fn get_into<O: AsyncWrite + Send + Unpin>(
        &self,
        id: &Self::Id,
        mut output: O,
    ) -> impl std::future::Future<Output = Result<u64>> + Send {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = id.clone();

        async move {
            Self::validate_key(&key)?;

            let resp = client.get_object().bucket(bucket).key(&key).send().await;

            let out = match resp {
                Ok(out) => out,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("NotFound")
                        || msg.contains("NoSuchKey")
                        || msg.contains("404")
                        || msg.contains("StatusCode(404)")
                    {
                        return Err(Error::NotFound(key));
                    }
                    return Err(Self::map_sdk_err(e));
                }
            };

            // Stream the S3 body into the provided Tokio writer.
            let mut stream = out.body;

            let mut written: u64 = 0;
            while let Some(chunk) = stream.next().await {
                let bytes = chunk.map_err(Self::map_sdk_err)?;
                output.write_all(&bytes).await?;
                written = written.saturating_add(bytes.len() as u64);
            }

            output.flush().await?;
            Ok(written)
        }
    }

    fn delete(&self, id: &Self::Id) -> impl std::future::Future<Output = Result<()>> + Send {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = id.clone();

        async move {
            Self::validate_key(&key)?;

            let resp = client.delete_object().bucket(bucket).key(key).send().await;

            match resp {
                Ok(_) => Ok(()),
                Err(e) => {
                    // Delete should be idempotent.
                    let msg = e.to_string();
                    if msg.contains("NotFound")
                        || msg.contains("NoSuchKey")
                        || msg.contains("404")
                        || msg.contains("StatusCode(404)")
                    {
                        Ok(())
                    } else {
                        Err(Self::map_sdk_err(e))
                    }
                }
            }
        }
    }

    fn list(
        &self,
        prefix: Option<&Self::Id>,
    ) -> impl std::future::Future<Output = Result<BoxStream<'_, Result<Self::Id>>>> + Send {
        let _ = prefix;
        async move {
            Err(Error::Generic(
                "S3Storage::list not implemented yet for get_into-based trait evolution"
                    .to_string(),
            ))
        }
    }
}

// Needed for `.next()` on the S3 byte stream
#[allow(unused_imports)]
use futures::StreamExt;
