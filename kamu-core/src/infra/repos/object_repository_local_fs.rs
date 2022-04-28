// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::object_repository::InternalError;
use crate::domain::*;
use opendatafabric::{Multicodec, Multihash};

use async_trait::async_trait;
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncRead, AsyncWriteExt};

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Pass a single type that configures digest algo, multicodec, and hash base
pub struct ObjectRepositoryLocalFS<D, const C: u32> {
    root: PathBuf,
    staging_path: PathBuf,
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
            staging_path: root.join(".pending"),
            root,
            _phantom: PhantomData,
        }
    }

    fn get_path(&self, hash: &Multihash) -> PathBuf {
        self.root.join(hash.to_multibase_string())
    }

    async fn write_stream_with_digest(
        &self,
        path: &Path,
        src: &mut AsyncReadObj,
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
    async fn contains(&self, hash: &Multihash) -> Result<bool, InternalError> {
        let path = self.get_path(hash);
        Ok(path.exists())
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Vec<u8>, GetError> {
        let path = self.get_path(hash);
        if !path.exists() {
            return Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            }));
        }
        let data = tokio::fs::read(path)
            .await
            .map_err(|e| GetError::Internal(e.into()))?;
        Ok(data)
    }

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let path = self.get_path(&hash);
        if !path.exists() {
            return Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            }));
        }

        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| GetError::Internal(e.into()))?;

        Ok(Box::new(file))
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

        let path = self.get_path(&hash);

        if path.exists() {
            return Ok(InsertResult {
                hash,
                already_existed: true,
            });
        }

        // Write to staging file
        tokio::fs::write(&self.staging_path, data)
            .await
            .map_err(|e| InsertError::Internal(e.into()))?;

        // Atomic move
        std::fs::rename(&self.staging_path, path).map_err(|e| InsertError::Internal(e.into()))?;

        Ok(InsertResult {
            hash,
            already_existed: false,
        })
    }

    async fn insert_stream<'a>(
        &'a self,
        src: &'a mut AsyncReadObj,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let actual_hash = self
            .write_stream_with_digest(&self.staging_path, src)
            .await
            .map_err(|e| InsertError::Internal(e.into()))?;

        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            actual_hash
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                tokio::fs::remove_file(&self.staging_path)
                    .await
                    .map_err(|e| InsertError::Internal(e.into()))?;

                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let path = self.get_path(&hash);

        if path.exists() {
            tokio::fs::remove_file(&self.staging_path)
                .await
                .map_err(|e| InsertError::Internal(e.into()))?;

            return Ok(InsertResult {
                hash,
                already_existed: true,
            });
        }

        // Atomic move
        std::fs::rename(&self.staging_path, path).map_err(|e| InsertError::Internal(e.into()))?;

        Ok(InsertResult {
            hash,
            already_existed: false,
        })
    }

    // async fn insert_file(
    //     &self,
    //     file: &Path,
    //     options: InsertOpts,
    // ) -> Result<Multihash, InsertError> {
    //     todo!()
    // }

    async fn delete(&self, hash: &Multihash) -> Result<(), InternalError> {
        let path = self.get_path(hash);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }
}
