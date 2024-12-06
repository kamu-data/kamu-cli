// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::DatasetReferenceRepository;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ManagedMetadataChainImpl {
    storage_chain_container: Arc<dyn AsMetadataChain>,
    dataset_id: odf::DatasetID,
    initial_state: State,
    modified_state: RwLock<Option<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
struct State {
    refs: BTreeMap<BlockRef, odf::Multihash>,
}

impl State {
    fn get_ref(&self, block_ref: &BlockRef) -> Result<&odf::Multihash, GetRefError> {
        match self.refs.get(block_ref) {
            Some(ref_hash) => Ok(ref_hash),
            None => Err(GetRefError::NotFound(RefNotFoundError {
                block_ref: block_ref.clone(),
            })),
        }
    }

    fn try_get_ref(&self, block_ref: &BlockRef) -> Option<&odf::Multihash> {
        self.refs.get(block_ref)
    }
}

impl PartialEq<State> for State {
    fn eq(&self, other: &State) -> bool {
        if self.refs.len() != other.refs.len() {
            return false;
        }

        itertools::equal(self.refs.iter(), other.refs.iter())
    }
}

impl Eq for State {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ManagedMetadataChainImpl {
    pub fn new(
        storage_chain_container: Arc<dyn AsMetadataChain>,
        dataset_id: odf::DatasetID,
        initial_refs: BTreeMap<BlockRef, odf::Multihash>,
    ) -> Self {
        Self {
            storage_chain_container,
            dataset_id,
            initial_state: State { refs: initial_refs },
            modified_state: RwLock::new(None),
        }
    }

    #[inline]
    fn get_storage_chain(&self) -> &dyn MetadataChain {
        self.storage_chain_container.as_metadata_chain()
    }

    pub async fn do_commit(self, transactional_catalog: &Catalog) -> Result<(), InternalError> {
        let maybe_modified_state = {
            let mut modified_guard = self.modified_state.write().unwrap();
            if modified_guard
                .as_ref()
                .is_some_and(|state| *state != self.initial_state)
            {
                modified_guard.take()
            } else {
                None
            }
        };

        if let Some(modified_state) = maybe_modified_state {
            let dataset_reference_repo = transactional_catalog
                .get_one::<dyn DatasetReferenceRepository>()
                .unwrap();

            for (block_ref, block_hash) in &modified_state.refs {
                let maybe_initial_hash = self.initial_state.try_get_ref(block_ref);
                dataset_reference_repo
                    .set_dataset_reference(
                        &self.dataset_id,
                        block_ref.as_str(),
                        maybe_initial_hash,
                        block_hash,
                    )
                    .await
                    .int_err()?;

                // Question: should this be at post-commit phase? maybe via Outbox?
                self.get_storage_chain()
                    .set_ref(block_ref, block_hash, SetRefOpts::default())
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MetadataChain for ManagedMetadataChainImpl {
    async fn resolve_ref(&self, r: &BlockRef) -> Result<odf::Multihash, GetRefError> {
        let modified_guard = self.modified_state.read().unwrap();
        let state = modified_guard.as_ref().unwrap_or(&self.initial_state);
        state.get_ref(r).cloned()
    }

    async fn contains_block(&self, hash: &odf::Multihash) -> Result<bool, ContainsBlockError> {
        self.get_storage_chain().contains_block(hash).await
    }

    async fn get_block_size(&self, hash: &odf::Multihash) -> Result<u64, GetBlockDataError> {
        self.get_storage_chain().get_block_size(hash).await
    }

    async fn get_block_bytes(&self, hash: &odf::Multihash) -> Result<Bytes, GetBlockDataError> {
        self.get_storage_chain().get_block_bytes(hash).await
    }

    async fn get_block(&self, hash: &odf::Multihash) -> Result<odf::MetadataBlock, GetBlockError> {
        self.get_storage_chain().get_block(hash).await
    }

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &odf::Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetRefError> {
        if opts.validate_block_present {
            match self.contains_block(hash).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(SetRefError::BlockNotFound(BlockNotFoundError {
                    hash: hash.clone(),
                })),
                Err(ContainsBlockError::Access(e)) => Err(SetRefError::Access(e)),
                Err(ContainsBlockError::Internal(e)) => Err(SetRefError::Internal(e)),
            }?;
        }

        if let Some(prev_expected) = opts.check_ref_is {
            let prev_actual = match self.resolve_ref(r).await {
                Ok(r) => Ok(Some(r)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(SetRefError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(SetRefError::Internal(e)),
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

        let mut modified_guard = self.modified_state.write().unwrap();
        let modified_state = modified_guard.get_or_insert_with(|| self.initial_state.clone());
        modified_state.refs.insert(r.clone(), hash.clone());
        Ok(())
    }

    /// Appends the block to the chain
    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<odf::Multihash, AppendError> {
        // Append new block to storage level
        let new_head = self
            .get_storage_chain()
            .append(
                block,
                AppendOpts {
                    // Make sure not setting ref at the moment
                    update_ref: None,
                    ..opts
                },
            )
            .await?;

        // If we have to update the ref, postone this until commit
        if let Some(block_ref) = opts.update_ref {
            self.set_ref(
                block_ref,
                &new_head,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: None,
                },
            )
            .await?;
        }

        Ok(new_head)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
