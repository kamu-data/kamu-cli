// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::UInt64Type;
use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::SessionContext;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::{
    ExportError,
    ExportFormat,
    ExportQueryError,
    ExportService,
    QueryService,
    SessionConfigOverrides,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExportServiceImpl {
    query_service: Arc<dyn QueryService>,
}

#[component(pub)]
#[interface(dyn ExportService)]
impl ExportServiceImpl {
    pub fn new(query_service: Arc<dyn QueryService>) -> Self {
        Self { query_service }
    }

    fn records_written(&self, batches: &Vec<RecordBatch>) -> Result<u64, ExportError> {
        let mut total = 0;
        for batch in batches {
            let maybe_count = batch
                .column_by_name("count")
                .and_then(|col| col.as_primitive_opt::<UInt64Type>())
                .and_then(|data| data.values().first());

            if let Some(count) = maybe_count {
                total += count;
            } else {
                return Err(ExportError::Internal(
                    "Failed to calculate number of exported rows".int_err(),
                ));
            }
        }
        Ok(total)
    }

    async fn execute(
        &self,
        ctx: SessionContext,
        sql_query: &str,
        path: &str,
        format: ExportFormat,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let df = ctx.sql(sql_query).await?;

        match format {
            ExportFormat::Parquet => {
                df.write_parquet(path, DataFrameWriteOptions::new(), None)
                    .await
            }
            ExportFormat::Csv => df.write_csv(path, DataFrameWriteOptions::new(), None).await,
            ExportFormat::NdJson => {
                df.write_json(path, DataFrameWriteOptions::new(), None)
                    .await
            }
        }
    }
}

#[async_trait::async_trait]
impl ExportService for ExportServiceImpl {
    async fn export_to_fs(
        &self,
        sql_query: &str,
        path: &str,
        format: ExportFormat,
        partition_row_count: Option<usize>,
    ) -> Result<u64, ExportError> {
        let config_overrides = SessionConfigOverrides {
            target_partitions: Some(1),
            soft_max_rows_per_output_file: partition_row_count,
            minimum_parallel_output_files: Some(1),
        };

        let ctx = self
            .query_service
            .create_session_with_config_overrides(config_overrides)
            .await
            .int_err()?;

        let result = self
            .execute(ctx, sql_query, path, format)
            .await
            .map_err(|err| match err {
                err @ (DataFusionError::Plan(_)
                | DataFusionError::SQL(_, _)
                | DataFusionError::Execution(_)
                | DataFusionError::SchemaError(..)) => ExportError::Query(ExportQueryError {
                    context: err.to_string(),
                    source: err.into(),
                }),
                other => ExportError::Internal(other.int_err()),
            })?;

        self.records_written(&result)
    }
}
