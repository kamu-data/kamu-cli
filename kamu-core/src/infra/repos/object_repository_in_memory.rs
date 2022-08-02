use core::panic;
use std::{collections::HashMap, sync::Mutex};

use crate::domain::*;
use async_trait::async_trait;
use bytes::Bytes;
use opendatafabric::Multihash;
use tokio::io::AsyncRead;

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
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        Ok(blocks_by_hash.contains_key(hash))
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
        unimplemented!()
    }

    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let mut blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        let hash = options.precomputed_hash.unwrap().clone();
        let bytes = Bytes::copy_from_slice(data);
        let already_existed = blocks_by_hash.insert(hash.clone(), bytes).is_some();
        Ok(InsertResult {
            hash,
            already_existed,
        })
    }

    async fn insert_stream<'a>(
        &'a self,
        _src: Box<AsyncReadObj>,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        unimplemented!()
    }

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let mut blocks_by_hash = self.blocks_by_hash.lock().unwrap();
        let res = blocks_by_hash.remove(hash);
        match res {
            Some(_) => Ok(()),
            None => panic!(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
