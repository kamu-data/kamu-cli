// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetReferenceRepository)]
pub struct SqliteDatasetReferenceRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceRepository for SqliteDatasetReferenceRepository {
    async fn has_any_references(&self) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_row = sqlx::query!(
            r#"
            SELECT 1 as count FROM dataset_references LIMIT 1
            "#,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.is_some())
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_str = stack_dataset_id.as_str();

        let block_ref_str = block_ref.as_str();

        let maybe_record = sqlx::query!(
            r#"
            SELECT block_hash
                FROM dataset_references
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id_str,
            block_ref_str,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(record) = maybe_record {
            Ok(odf::Multihash::from_multibase(&record.block_hash).unwrap())
        } else {
            Err(DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: *block_ref,
            }
            .into())
        }
    }

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(odf::BlockRef, odf::Multihash)>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_str = stack_dataset_id.as_str();

        let records = sqlx::query!(
            r#"
            SELECT block_ref_name, block_hash
                FROM dataset_references
                WHERE dataset_id = $1
            "#,
            stack_dataset_id_str,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let mut res = Vec::new();
        for record in records {
            res.push((
                odf::BlockRef::from_str(record.block_ref_name.as_str())?,
                odf::Multihash::from_multibase(&record.block_hash).int_err()?,
            ));
        }

        Ok(res)
    }

    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let query_result = {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let dataset_id = dataset_id.to_string();
            let dataset_id_str = dataset_id.as_str();

            let block_ref_str = block_ref.as_str();

            let maybe_prev_block_hash = maybe_prev_block_hash.map(ToString::to_string);
            let maybe_prev_block_hash_str = maybe_prev_block_hash.as_deref();

            let block_hash = block_hash.to_string();
            let block_hash_str = block_hash.as_str();

            sqlx::query!(
                r#"
                INSERT INTO dataset_references (dataset_id, block_ref_name, block_hash)
                    VALUES ($1, $2, $3)
                    ON CONFLICT(dataset_id, block_ref_name)
                    DO UPDATE SET block_hash = $3 WHERE dataset_references.block_hash = $4
                "#,
                dataset_id_str,
                block_ref_str,
                block_hash_str,
                maybe_prev_block_hash_str,
            )
            .execute(connection_mut)
            .await
            .int_err()?
        };

        if query_result.rows_affected() == 0 {
            let maybe_actual_block_hash =
                match self.get_dataset_reference(dataset_id, block_ref).await {
                    Ok(actual_block_hash) => Some(actual_block_hash),
                    Err(GetDatasetReferenceError::NotFound(_)) => None,
                    Err(GetDatasetReferenceError::Internal(e)) => return Err(e.into()),
                };

            assert_ne!(maybe_actual_block_hash.as_ref(), maybe_prev_block_hash);
            Err(SetDatasetReferenceError::CASFailed(
                DatasetReferenceCASError::new(
                    dataset_id,
                    block_ref,
                    maybe_prev_block_hash,
                    maybe_actual_block_hash.as_ref(),
                ),
            ))
        } else {
            Ok(())
        }
    }

    async fn remove_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), RemoveDatasetReferenceError> {
        let query_result = {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let dataset_id = dataset_id.to_string();
            let dataset_id_str = dataset_id.as_str();

            let block_ref_str = block_ref.as_str();

            sqlx::query!(
                r#"
                DELETE FROM dataset_references
                    WHERE dataset_id = $1 AND block_ref_name = $2
                "#,
                dataset_id_str,
                block_ref_str,
            )
            .execute(connection_mut)
            .await
            .int_err()?
        };

        if query_result.rows_affected() == 0 {
            Err(RemoveDatasetReferenceError::NotFound(
                DatasetReferenceNotFoundError {
                    dataset_id: dataset_id.clone(),
                    block_ref: *block_ref,
                },
            ))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
