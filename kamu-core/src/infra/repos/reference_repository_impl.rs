// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use opendatafabric::Multihash;

use crate::domain::repos::named_object_repository::{DeleteError, GetError};
use crate::domain::repos::reference_repository::SetRefError;
use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryImpl<R> {
    repo: R,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<R> ReferenceRepositoryImpl<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<R> ReferenceRepository for ReferenceRepositoryImpl<R>
where
    R: NamedObjectRepository + Send + Sync,
{
    async fn get(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        let data = match self.repo.get(&r.as_str()).await {
            Ok(data) => Ok(data),
            Err(GetError::NotFound(_)) => Err(GetRefError::NotFound(RefNotFoundError {
                block_ref: r.clone(),
            })),
            Err(GetError::Access(e)) => Err(GetRefError::Access(e)),
            Err(GetError::Internal(e)) => Err(GetRefError::Internal(e)),
        }?;
        let text = std::str::from_utf8(&data[..]).int_err()?;
        let hash = Multihash::from_multibase_str(&text).int_err()?;
        Ok(hash)
    }

    async fn set(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError> {
        let multibase = hash.to_multibase_string();
        match self.repo.set(&r.as_str(), multibase.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(SetError::Access(e)) => Err(SetRefError::Access(e)),
            Err(SetError::Internal(e)) => Err(SetRefError::Internal(e)),
        }
    }

    async fn delete(&self, r: &BlockRef) -> Result<(), DeleteRefError> {
        match self.repo.delete(r.as_str()).await {
            Ok(()) => Ok(()),
            Err(DeleteError::Access(e)) => Err(DeleteRefError::Access(e)),
            Err(DeleteError::Internal(e)) => Err(DeleteRefError::Internal(e)),
        }
    }
}
