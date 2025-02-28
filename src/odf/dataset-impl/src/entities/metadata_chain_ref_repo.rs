// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MetadataChainReferenceRepository: Send + Sync {
    fn detach_from_transaction(&self) {
        // Nothing to do by default
    }

    fn reattach_to_transaction(&self, _catalog: &dill::Catalog) {
        // Nothing to do by default
    }

    async fn get_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError>;

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        check_ref_is: Option<Option<&'a Multihash>>,
    ) -> Result<(), SetChainRefError>;

    fn as_raw_ref_repo(&self) -> &dyn ReferenceRepository;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainReferenceRepositoryImpl<RefRepo>
where
    RefRepo: ReferenceRepository + Send + Sync,
{
    ref_repo: RefRepo,
}

impl<RefRepo> MetadataChainReferenceRepositoryImpl<RefRepo>
where
    RefRepo: ReferenceRepository + Send + Sync,
{
    pub fn new(ref_repo: RefRepo) -> Self {
        Self { ref_repo }
    }
}

#[async_trait::async_trait]
impl<RefRepo> MetadataChainReferenceRepository for MetadataChainReferenceRepositoryImpl<RefRepo>
where
    RefRepo: ReferenceRepository + Send + Sync,
{
    async fn get_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        self.ref_repo.get(r.as_str()).await
    }

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        check_ref_is: Option<Option<&'a Multihash>>,
    ) -> Result<(), SetChainRefError> {
        // TODO: CONCURRENCY: Implement true CAS
        if let Some(prev_expected) = check_ref_is {
            let prev_actual = match self.ref_repo.get(r.as_str()).await {
                Ok(r) => Ok(Some(r)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(SetChainRefError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(SetChainRefError::Internal(e)),
            }?;
            if prev_expected != prev_actual.as_ref() {
                return Err(RefCASError {
                    reference: r.clone(),
                    expected: prev_expected.cloned(),
                    actual: prev_actual,
                }
                .into());
            }
        }

        self.ref_repo.set(r.as_str(), hash).await?;

        Ok(())
    }

    fn as_raw_ref_repo(&self) -> &dyn ReferenceRepository {
        &self.ref_repo
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
