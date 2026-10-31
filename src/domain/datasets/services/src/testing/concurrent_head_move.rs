// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionManager, DatabaseTransactionRunner};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::DatasetRegistry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Moves a dataset's `HEAD` as a concurrent writer does: in a transaction of
/// its own, committed before the writer it races with commits
pub(crate) struct ConcurrentHeadMove {
    catalog: dill::Catalog,
    concurrent_head: odf::Multihash,
}

impl ConcurrentHeadMove {
    /// `catalog` resolves `DatasetRegistry` once a transaction is added to it
    pub fn new(catalog: &dill::Catalog, concurrent_head: odf::Multihash) -> Self {
        Self {
            catalog: catalog.clone(),
            concurrent_head,
        }
    }

    pub async fn apply(self, dataset_handle: &odf::DatasetHandle) {
        // With no transaction manager there are no transactions to race with
        if self
            .catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .is_err()
        {
            let dataset_registry = self.catalog.get_one::<dyn DatasetRegistry>().unwrap();
            Self::set_head(dataset_registry, dataset_handle, &self.concurrent_head)
                .await
                .unwrap();
            return;
        }

        let concurrent_head = self.concurrent_head;
        DatabaseTransactionRunner::new(self.catalog)
            .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| async move {
                Self::set_head(dataset_registry, dataset_handle, &concurrent_head).await
            })
            .await
            .unwrap();
    }

    async fn set_head(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_handle: &odf::DatasetHandle,
        concurrent_head: &odf::Multihash,
    ) -> Result<(), InternalError> {
        dataset_registry
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
