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

pub struct PostgresDatasetReferenceRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn DatasetReferenceRepository)]
impl PostgresDatasetReferenceRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceRepository for PostgresDatasetReferenceRepository {
    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let query_result = {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            sqlx::query!(
                r#"
                INSERT INTO dataset_references (dataset_id, block_ref_name, block_hash)
                    VALUES ($1, $2, $3)
                    ON CONFLICT(dataset_id, block_ref_name)
                    DO UPDATE SET block_hash = $3 WHERE dataset_references.block_hash = $4
                "#,
                dataset_id.to_string(),
                block_ref_name,
                block_hash.to_string(),
                maybe_prev_block_hash.map(ToString::to_string),
            )
            .execute(connection_mut)
            .await
            .int_err()?
        };

        if query_result.rows_affected() == 0 {
            let maybe_actual_block_hash =
                match self.get_dataset_reference(dataset_id, block_ref_name).await {
                    Ok(actual_block_hash) => Some(actual_block_hash),
                    Err(GetDatasetReferenceError::NotFound(_)) => None,
                    Err(GetDatasetReferenceError::Internal(e)) => return Err(e.into()),
                };

            assert_ne!(maybe_actual_block_hash.as_ref(), maybe_prev_block_hash);
            Err(SetDatasetReferenceError::CASFailed(
                DatasetReferenceCASError::new(
                    dataset_id,
                    block_ref_name,
                    maybe_prev_block_hash,
                    maybe_actual_block_hash.as_ref(),
                ),
            ))
        } else {
            Ok(())
        }
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let maybe_record = sqlx::query!(
            r#"
            SELECT block_hash
                FROM dataset_references
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id.as_str(),
            block_ref_name
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(record) = maybe_record {
            Ok(odf::Multihash::from_multibase(&record.block_hash).unwrap())
        } else {
            Err(DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref_name: block_ref_name.to_string(),
            }
            .into())
        }
    }

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(String, odf::Multihash)>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let records = sqlx::query!(
            r#"
            SELECT block_ref_name, block_hash
                FROM dataset_references
                WHERE dataset_id = $1
            "#,
            stack_dataset_id.as_str(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|record| {
                (
                    record.block_ref_name,
                    odf::Multihash::from_multibase(&record.block_hash).unwrap(),
                )
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////