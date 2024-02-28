// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use kamu_core::*;
use opendatafabric::{Multicodec, Multihash};
use tokio::io::AsyncRead;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ObjectRepositoryInMemory {
    blocks_by_hash: Mutex<HashMap<Multihash, Bytes>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ObjectRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            blocks_by_hash: Mutex::new(HashMap::new()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl ObjectRepository for ObjectRepositoryInMemory {
    fn protocol(&self) -> ObjectRepositoryProtocol {
        ObjectRepositoryProtocol::Memory
    }

    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        Ok(blocks_by_hash.contains_key(hash))
    }

    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        let blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        let res = blocks_by_hash.get(hash);
        match res {
            Some(bytes) => Ok(bytes.len() as u64),
            None => Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            })),
        }
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        let res = blocks_by_hash.get(hash);
        match res {
            Some(bytes) => Ok(bytes.clone()),
            None => Err(GetError::NotFound(ObjectNotFoundError {
                hash: hash.clone(),
            })),
        }
    }

    async fn get_stream(&self, _hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        panic!("get_stream not allowed for in-memory repository");
    }

    async fn get_internal_url(&self, _hash: &Multihash) -> Url {
        panic!("get_internal_url not allowed for in-memory repository");
    }

    async fn get_external_download_url(
        &self,
        _hash: &Multihash,
        _opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        Err(GetExternalUrlError::NotSupported)
    }

    async fn get_external_upload_url(
        &self,
        _hash: &Multihash,
        _opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        Err(GetExternalUrlError::NotSupported)
    }

    fn get_bytes_hash(&self, data: &[u8]) -> Result<Multihash, GetBytesHashError> {
        Ok(Multihash::from_digest::<sha3::Sha3_256>(
            Multicodec::Sha3_256,
            data,
        ))
    }

    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let mut blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            self.get_bytes_hash(data)?
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let bytes = Bytes::copy_from_slice(data);
        blocks_by_hash.insert(hash.clone(), bytes);
        Ok(InsertResult { hash })
    }

    async fn insert_stream<'a>(
        &'a self,
        _src: Box<AsyncReadObj>,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        panic!("insert_stream not allowed for in-memory repository");
    }

    async fn insert_file_move<'a>(
        &'a self,
        _src: &Path,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        panic!("insert_file_move not allowed for in-memory repository");
    }

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let mut blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        blocks_by_hash.remove(hash);
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
