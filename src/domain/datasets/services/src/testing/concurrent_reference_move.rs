// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionManager, DatabaseTransactionRunner};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetReferenceRepository, DatasetRegistry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Moves a dataset's reference as a concurrent writer does: in a transaction
/// of its own, committed before the writer it races with commits
pub(crate) struct ConcurrentReferenceMove {
    catalog: dill::Catalog,
    /// `None` removes the reference instead of moving it
    concurrent_block_hash: Option<odf::Multihash>,
}

impl ConcurrentReferenceMove {
    /// `catalog` resolves `DatasetRegistry` once a transaction is added to it
    pub fn new(catalog: &dill::Catalog, concurrent_block_hash: odf::Multihash) -> Self {
        Self {
            catalog: catalog.clone(),
            concurrent_block_hash: Some(concurrent_block_hash),
        }
    }

    /// Removes the reference; `catalog` resolves `DatasetReferenceRepository` once a
    /// transaction is added to it
    pub fn removing(catalog: &dill::Catalog) -> Self {
        Self {
            catalog: catalog.clone(),
            concurrent_block_hash: None,
        }
    }

    pub async fn apply(self, dataset_handle: &odf::DatasetHandle, block_ref: &odf::BlockRef) {
        // With no transaction manager there are no transactions to race with
        if self
            .catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .is_err()
        {
            Self::move_reference(
                &self.catalog,
                dataset_handle,
                block_ref,
                self.concurrent_block_hash.as_ref(),
            )
            .await
            .unwrap();
            return;
        }

        let concurrent_block_hash = self.concurrent_block_hash;
        DatabaseTransactionRunner::new(self.catalog)
            .transactional(|transaction_catalog| async move {
                Self::move_reference(
                    &transaction_catalog,
                    dataset_handle,
                    block_ref,
                    concurrent_block_hash.as_ref(),
                )
                .await
            })
            .await
            .unwrap();
    }

    async fn move_reference(
        catalog: &dill::Catalog,
        dataset_handle: &odf::DatasetHandle,
        block_ref: &odf::BlockRef,
        concurrent_block_hash: Option<&odf::Multihash>,
    ) -> Result<(), InternalError> {
        let Some(concurrent_block_hash) = concurrent_block_hash else {
            return catalog
                .get_one::<dyn DatasetReferenceRepository>()
                .int_err()?
                .remove_dataset_reference(&dataset_handle.id, block_ref)
                .await
                .int_err();
        };

        catalog
            .get_one::<dyn DatasetRegistry>()
            .int_err()?
            .get_dataset_by_handle(dataset_handle)
            .await
            .as_metadata_chain()
            .set_ref(
                block_ref,
                concurrent_block_hash,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
