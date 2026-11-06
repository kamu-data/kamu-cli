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

/// Moves a dataset's `HEAD` as a concurrent writer does: in a transaction of
/// its own, committed before the writer it races with commits
pub(crate) struct ConcurrentHeadMove {
    catalog: dill::Catalog,
    /// `None` removes `HEAD` instead of moving it
    concurrent_head: Option<odf::Multihash>,
}

impl ConcurrentHeadMove {
    /// `catalog` resolves `DatasetRegistry` once a transaction is added to it
    pub fn new(catalog: &dill::Catalog, concurrent_head: odf::Multihash) -> Self {
        Self {
            catalog: catalog.clone(),
            concurrent_head: Some(concurrent_head),
        }
    }

    /// Removes `HEAD`; `catalog` resolves `DatasetReferenceRepository` once a
    /// transaction is added to it
    pub fn removing(catalog: &dill::Catalog) -> Self {
        Self {
            catalog: catalog.clone(),
            concurrent_head: None,
        }
    }

    pub async fn apply(self, dataset_handle: &odf::DatasetHandle) {
        // With no transaction manager there are no transactions to race with
        if self
            .catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .is_err()
        {
            Self::move_head(&self.catalog, dataset_handle, self.concurrent_head.as_ref())
                .await
                .unwrap();
            return;
        }

        let concurrent_head = self.concurrent_head;
        DatabaseTransactionRunner::new(self.catalog)
            .transactional(|transaction_catalog| async move {
                Self::move_head(
                    &transaction_catalog,
                    dataset_handle,
                    concurrent_head.as_ref(),
                )
                .await
            })
            .await
            .unwrap();
    }

    async fn move_head(
        catalog: &dill::Catalog,
        dataset_handle: &odf::DatasetHandle,
        concurrent_head: Option<&odf::Multihash>,
    ) -> Result<(), InternalError> {
        let Some(concurrent_head) = concurrent_head else {
            return catalog
                .get_one::<dyn DatasetReferenceRepository>()
                .int_err()?
                .remove_dataset_reference(&dataset_handle.id, &odf::BlockRef::Head)
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
                &odf::BlockRef::Head,
                concurrent_head,
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
