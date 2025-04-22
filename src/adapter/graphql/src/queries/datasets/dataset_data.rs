// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core as domain;

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetData<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetData<'a> {
    const DEFAULT_TAIL_LIMIT: u64 = 20;

    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Total number of records in this dataset
    #[tracing::instrument(level = "info", name = DatasetData_num_records_total, skip_all)]
    async fn num_records_total(&self, ctx: &Context<'_>) -> Result<u64> {
        let dataset_statistics = self.dataset_request_state.dataset_statistics(ctx).await?;
        Ok(dataset_statistics.num_records)
    }

    /// An estimated size of data on disk not accounting for replication or
    /// caching
    #[tracing::instrument(level = "info", name = DatasetData_estimated_size, skip_all)]
    async fn estimated_size(&self, ctx: &Context<'_>) -> Result<u64> {
        let dataset_statistics = self.dataset_request_state.dataset_statistics(ctx).await?;
        Ok(dataset_statistics.data_size)
    }

    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to SQL query like:
    ///
    /// ```text
    /// select * from (
    ///   select
    ///     *
    ///   from dataset
    ///   order by offset desc
    ///   limit lim
    ///   offset skip
    /// )
    /// order by offset
    /// ```
    #[tracing::instrument(level = "info", name = DatasetData_tail, skip_all, fields(?skip, ?limit, ?data_format, ?schema_format))]
    async fn tail(
        &self,
        ctx: &Context<'_>,
        skip: Option<u64>,
        limit: Option<u64>,
        data_format: Option<DataBatchFormat>,
        schema_format: Option<DataSchemaFormat>,
    ) -> Result<DataQueryResult> {
        tracing::debug!(?data_format, ?schema_format, ?skip, ?limit, "Tail query");

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: Default to JsonSoA format once implemented
        let data_format = data_format.unwrap_or(DataBatchFormat::Json);
        let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
        let limit = limit.unwrap_or(Self::DEFAULT_TAIL_LIMIT);

        let tail_result = query_svc
            .tail(
                &self.dataset_request_state.dataset_handle().as_local_ref(),
                skip.unwrap_or(0),
                limit,
            )
            .await;

        let df = match tail_result {
            Ok(r) => r.df,
            Err(domain::QueryError::DatasetSchemaNotAvailable(_)) => {
                return Ok(DataQueryResult::no_schema_yet(data_format, limit));
            }
            Err(err) => {
                return DataQueryResult::from_query_error(err);
            }
        };

        let schema = DataSchema::from_data_frame_schema(df.schema(), schema_format)?;
        let record_batches = match df.collect().await {
            Ok(rb) => rb,
            Err(err) => return DataQueryResult::from_query_error(err.into()),
        };

        let data = DataBatch::from_records(&record_batches, data_format)?;

        Ok(DataQueryResult::success(Some(schema), data, None, limit))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
