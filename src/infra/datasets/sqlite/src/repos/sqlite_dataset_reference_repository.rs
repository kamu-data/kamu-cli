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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteDatasetReferenceRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn DatasetReferenceRepository)]
impl SqliteDatasetReferenceRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceRepository for SqliteDatasetReferenceRepository {
    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
        block_ptr: BlockPointer,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();
        let dataset_id_str = dataset_id.as_str();

        let block_hash = block_ptr.block_hash.to_string();
        let block_hash_str = block_hash.as_str();

        let block_sequence_number = i64::try_from(block_ptr.block_sequence_number).unwrap();

        sqlx::query!(
            r#"
            INSERT INTO dataset_references (dataset_id, block_ref_name, block_hash, block_sequence_number)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT(dataset_id, block_ref_name)
                DO UPDATE SET block_hash = $3, block_sequence_number = $4
            "#,
            dataset_id_str,
            block_ref_name,
            block_hash_str,
            block_sequence_number,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
    ) -> Result<BlockPointer, GetDatasetReferenceError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_str = stack_dataset_id.as_str();

        let maybe_block_pointer_record = sqlx::query!(
            r#"
            SELECT block_hash, block_sequence_number
                FROM dataset_references
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id_str,
            block_ref_name
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(block_pointer_record) = maybe_block_pointer_record {
            Ok(BlockPointer {
                block_hash: odf::Multihash::from_multibase(&block_pointer_record.block_hash)
                    .unwrap(),
                block_sequence_number: u64::try_from(block_pointer_record.block_sequence_number)
                    .unwrap(),
            })
        } else {
            Err(DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref_name: block_ref_name.to_string(),
            }
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
