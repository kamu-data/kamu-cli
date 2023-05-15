// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use opendatafabric::{Multicodec, Multihash};
use tokio::io::{AsyncRead, AsyncWriteExt};

use crate::domain::*;
use crate::infra::get_staging_name;

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Pass a single type that configures digest algo, multicodec, and hash
// base
pub struct ObjectRepositoryLocalFS<D, const C: u32> {
    root: PathBuf,
    _phantom: PhantomData<D>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D, const C: u32> ObjectRepositoryLocalFS<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    pub fn new<P>(root: P) -> Self
    where
        P: Into<PathBuf>,
    {
        let root = root.into();
        Self {
            root,
            _phantom: PhantomData,
        }
    }

    fn get_path(&self, hash: &Multihash) -> PathBuf {
        self.root.join(hash.to_multibase_string())
    }

    // TODO: We should newtype Path and ensure repositoris are created for
    // directories that exist
    fn get_path_write(&self, hash: &Multihash) -> Result<PathBuf, std::io::Error> {
        if !self.root.exists() {
            std::fs::create_dir_all(&self.root)?;
        }
        Ok(self.get_path(hash))
    }

    // TODO: Cleanup procedure for orphaned staging files?
    fn get_staging_path(&self) -> Result<PathBuf, std::io::Error> {
        if !self.root.exists() {
            std::fs::create_dir_all(&self.root)?;
        }

        Ok(self.root.join(get_staging_name()))
    }

    async fn write_stream_with_digest<'a>(
        &'a self,
        path: &'a Path,
        mut src: Box<AsyncReadObj>,
    ) -> std::io::Result<Multihash> {
        use tokio::io::AsyncReadExt;

        let mut file = tokio::fs::File::create(path).await?;

        let mut digest = D::new();
        let mut buf = [0u8; 2048];

        loop {
            let read = src.read(&mut buf).await?;

            if read == 0 {
                break;
            }

            digest.update(&buf[..read]);
            file.write_all(&buf[..read]).await?;
        }

        file.flush().await?;

        Ok(Multihash::new(
            Multicodec::try_from(C).unwrap(),
            &digest.finalize(),
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<D, const C: u32> ObjectRepository for ObjectRepositoryLocalFS<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let path = self.get_path(hash);

        tracing::debug!(?path, "Checking for object");

        Ok(path.exists())
    }

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        let path = self.get_path(hash);

        tracing::debug!(?path, "Reading object size");

        if !path.exists() {
            return Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            }));
        }
        let metadata = tokio::fs::metadata(path).await.int_err()?;
        Ok(metadata.len())
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let path = self.get_path(hash);

        tracing::debug!(?path, "Reading object");

        if !path.exists() {
            return Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            }));
        }
        let data = tokio::fs::read(path).await.int_err()?;

        Ok(Bytes::from(data))
    }

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let path = self.get_path(&hash);

        tracing::debug!(?path, "Reading object stream");

        if !path.exists() {
            return Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            }));
        }

        let file = tokio::fs::File::open(path).await.int_err()?;

        Ok(Box::new(file))
    }

    async fn get_download_url(
        &self,
        _hash: &Multihash,
        _opts: TransferOpts,
    ) -> Result<GetTransferUrlResult, GetTransferUrlError> {
        Err(GetTransferUrlError::NotSupported)
    }

    async fn get_upload_url(
        &self,
        _hash: &Multihash,
        _opts: TransferOpts,
    ) -> Result<GetTransferUrlResult, GetTransferUrlError> {
        Err(GetTransferUrlError::NotSupported)
    }

    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            Multihash::from_digest::<D>(Multicodec::try_from(C).unwrap(), data)
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let path = self.get_path_write(&hash).int_err()?;

        tracing::debug!(?path, "Inserting object");

        if path.exists() {
            return Ok(InsertResult { hash });
        }

        let staging_path = self.get_staging_path().int_err()?;

        // Write to staging file
        tokio::fs::write(&staging_path, data).await.int_err()?;

        // Atomic move
        std::fs::rename(&staging_path, path).int_err()?;

        Ok(InsertResult { hash })
    }

    async fn insert_stream<'a>(
        &'a self,
        src: Box<AsyncReadObj>,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let staging_path = self.get_staging_path().int_err()?;

        let actual_hash = self
            .write_stream_with_digest(&staging_path, src)
            .await
            .int_err()?;

        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            actual_hash
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                tokio::fs::remove_file(&staging_path).await.int_err()?;

                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let path = self.get_path_write(&hash).int_err()?;

        tracing::debug!(?path, "Inserting object stream");

        if path.exists() {
            tokio::fs::remove_file(&staging_path).await.int_err()?;

            return Ok(InsertResult { hash });
        }

        // Atomic move
        std::fs::rename(&staging_path, path).int_err()?;

        Ok(InsertResult { hash })
    }

    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            crate::infra::utils::data_utils::get_file_physical_hash(src).int_err()?
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let path = self.get_path_write(&hash).int_err()?;

        tracing::debug!(?src, ?path, "Inserting object from file");
        tracing::debug!("{} {}", path.parent().unwrap().exists(), src.exists());

        if path.exists() {
            return Ok(InsertResult { hash });
        }

        // Atomic move
        std::fs::rename(src, path).int_err()?;

        Ok(InsertResult { hash })
    }

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let path = self.get_path(hash);

        tracing::debug!(?path, "Deleting object");

        if path.exists() {
            tokio::fs::remove_file(&path).await.int_err()?;
        }
        Ok(())
    }
}
