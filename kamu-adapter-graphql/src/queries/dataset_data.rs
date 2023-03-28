// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use kamu::domain;
use kamu::domain::GetSummaryOpts;
use kamu::domain::QueryError;
use opendatafabric as odf;
use tracing::debug;

pub struct DatasetData {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetData {
    const DEFAULT_TAIL_LIMIT: u64 = 20;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Total number of records in this dataset
    async fn num_records_total(&self, ctx: &Context<'_>) -> Result<u64> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = local_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await?;
        let summary = dataset.get_summary(GetSummaryOpts::default()).await?;
        Ok(summary.num_records)
    }

    /// An estimated size of data on disk not accounting for replication or caching
    async fn estimated_size(&self, ctx: &Context<'_>) -> Result<u64> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = local_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await?;
        let summary = dataset.get_summary(GetSummaryOpts::default()).await?;
        Ok(summary.data_size)
    }

    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to the SQL query: `SELECT * FROM dataset ORDER BY event_time DESC LIMIT N`
    async fn tail(
        &self,
        ctx: &Context<'_>,
        limit: Option<u64>,
        data_format: Option<DataBatchFormat>,
        schema_format: Option<DataSchemaFormat>,
    ) -> Result<DataQueryResult> {
        // TODO: Default to JsonSoA format once implemented
        let data_format = data_format.unwrap_or(DataBatchFormat::Json);
        let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
        let limit = limit.unwrap_or(Self::DEFAULT_TAIL_LIMIT);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        let df = match query_svc
            .tail(&self.dataset_handle.as_local_ref(), limit)
            .await
        {
            Ok(r) => r,
            Err(err) => {
                if let QueryError::DatasetSchemaNotAvailable(_) = err {
                    return Ok(DataQueryResult::no_schema_yet(data_format, limit));
                } else {
                    debug!(?err, "Query error");
                    return Ok(err.into());
                }
            }
        };

        let schema = DataSchema::from_data_frame_schema(df.schema(), schema_format)?;
        let record_batches = match df.collect().await {
            Ok(rb) => rb,
            Err(e) => return Ok(e.into()),
        };
        let data = DataBatch::from_records(&record_batches, data_format)?;

        Ok(DataQueryResult::success(Some(schema), data, limit))
    }
}
