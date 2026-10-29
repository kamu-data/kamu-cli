// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use internal_error::ResultIntoInternal;
use odf_metadata::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReferenceRepositoryImpl<R> {
    repo: R,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> ReferenceRepositoryImpl<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<R> ReferenceRepository for ReferenceRepositoryImpl<R>
where
    R: NamedObjectRepository + Send + Sync,
{
    async fn get(&self, r: &str) -> Result<Multihash, GetRefError> {
        let data = match self.repo.get(r).await {
            Ok(data) => Ok(data),
            Err(GetNamedError::NotFound(_)) => Err(GetRefError::NotFound(RefNotFoundError {
                block_ref_name: r.to_string(),
            })),
            Err(GetNamedError::Access(e)) => Err(GetRefError::Access(e)),
            Err(GetNamedError::Internal(e)) => Err(GetRefError::Internal(e)),
        }?;
        let text = std::str::from_utf8(&data[..]).int_err()?;
        let hash = Multihash::from_multibase(text).int_err()?;
        Ok(hash)
    }

    async fn set(&self, r: &str, hash: &Multihash) -> Result<(), SetRefError> {
        let multibase = hash.as_multibase().to_stack_string();
        match self.repo.set(r, multibase.as_bytes()).await {
            Ok(()) => Ok(()),
            Err(SetNamedError::Access(e)) => Err(SetRefError::Access(e)),
            Err(SetNamedError::Internal(e)) => Err(SetRefError::Internal(e)),
        }
    }

    async fn compare_and_set(
        &self,
        r: &str,
        expected: Option<&Multihash>,
        hash: &Multihash,
    ) -> Result<(), CompareAndSetRefError> {
        let mismatch = || {
            CompareAndSetRefError::Mismatch(RefMismatchError {
                block_ref_name: r.to_string(),
            })
        };

        // The same hash can be stored in another multibase encoding, so hashes are
        // compared here, and the conditional write expects the exact stored text
        let expected_text = match expected {
            None => None,
            Some(expected) => {
                let stored = match self.repo.get(r).await {
                    Ok(stored) => stored,
                    Err(GetNamedError::NotFound(_)) => return Err(mismatch()),
                    Err(GetNamedError::Access(e)) => return Err(CompareAndSetRefError::Access(e)),
                    Err(GetNamedError::Internal(e)) => {
                        return Err(CompareAndSetRefError::Internal(e));
                    }
                };
                let text = std::str::from_utf8(&stored[..]).int_err()?;
                if Multihash::from_multibase(text).int_err()? != *expected {
                    return Err(mismatch());
                }
                Some(stored)
            }
        };

        let multibase = hash.as_multibase().to_stack_string();
        match self
            .repo
            .compare_and_set(r, expected_text.as_deref(), multibase.as_bytes())
            .await
        {
            Ok(()) => Ok(()),
            Err(CompareAndSetNamedError::Mismatch(_)) => Err(mismatch()),
            Err(CompareAndSetNamedError::NotSupported) => Err(CompareAndSetRefError::NotSupported),
            Err(CompareAndSetNamedError::Access(e)) => Err(CompareAndSetRefError::Access(e)),
            Err(CompareAndSetNamedError::Internal(e)) => Err(CompareAndSetRefError::Internal(e)),
        }
    }

    async fn delete(&self, r: &str) -> Result<(), DeleteRefError> {
        match self.repo.delete(r).await {
            Ok(()) => Ok(()),
            Err(DeleteNamedError::Access(e)) => Err(DeleteRefError::Access(e)),
            Err(DeleteNamedError::Internal(e)) => Err(DeleteRefError::Internal(e)),
        }
    }
}
