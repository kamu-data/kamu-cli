// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetStatisticsRepository)]
pub struct PostgresDatasetStatisticsRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetStatisticsRepository for PostgresDatasetStatisticsRepository {
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_row = sqlx::query!(r#"SELECT 1 as count FROM dataset_statistics LIMIT 1"#)
            .fetch_optional(connection_mut)
            .await
            .int_err()?;

        Ok(maybe_row.is_some())
    }

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let maybe_record = sqlx::query!(
            r#"
            SELECT last_pulled, num_records, data_size, checkpoints_size FROM dataset_statistics
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id.as_str(),
            block_ref.as_str(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(record) = maybe_record {
            Ok(DatasetStatistics {
                last_pulled: record.last_pulled,
                num_records: u64::try_from(record.num_records).unwrap(),
                data_size: u64::try_from(record.data_size).unwrap(),
                checkpoints_size: u64::try_from(record.checkpoints_size).unwrap(),
            })
        } else {
            Err(DatasetStatisticsNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: *block_ref,
            }
            .into())
        }
    }

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO dataset_statistics (dataset_id, block_ref_name, last_pulled, num_records, data_size, checkpoints_size)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (dataset_id, block_ref_name)
                DO UPDATE SET
                    last_pulled = EXCLUDED.last_pulled,
                    num_records = EXCLUDED.num_records,
                    data_size = EXCLUDED.data_size,
                    checkpoints_size = EXCLUDED.checkpoints_size
            "#,
            dataset_id.to_string(),
            block_ref.as_str(),
            statistics.last_pulled,
            i64::try_from(statistics.num_records).unwrap(),
            i64::try_from(statistics.data_size).unwrap(),
            i64::try_from(statistics.checkpoints_size).unwrap(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
