// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::InternalError;
use kamu_datasets::*;
use opendatafabric::{DatasetID, MetadataEventTypeFlags};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresDatasetKeyBlocksRepository {
    _transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlocksRepository)]
impl PostgresDatasetKeyBlocksRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            _transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlocksRepository for PostgresDatasetKeyBlocksRepository {
    async fn save_key_dataset_block(
        &self,
        _dataset_id: &DatasetID,
        _key_block_row: DatasetKeyBlockRow,
    ) -> Result<(), InternalError> {
        // TODO
        unimplemented!()
    }

    async fn drop_key_dataset_blocks_after(
        &self,
        _dataset_id: &DatasetID,
        _sequence_number: u64,
    ) -> Result<(), InternalError> {
        // TODO
        unimplemented!()
    }

    async fn try_loading_key_dataset_blocks(
        &self,
        _dataset_id: &DatasetID,
        _flags: MetadataEventTypeFlags,
    ) -> Result<Vec<DatasetKeyBlockRow>, InternalError> {
        // TODO
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
