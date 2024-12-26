// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::UInt64Type;
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::logical_expr::Partitioning;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::{ExportError, ExportFormat, ExportOptions, ExportService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExportServiceImpl {}

#[component(pub)]
#[interface(dyn ExportService)]
impl ExportServiceImpl {
    pub fn new() -> Self {
        Self {}
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
}

#[async_trait::async_trait]
impl ExportService for ExportServiceImpl {
    async fn export_to_fs(
        &self,
        df: DataFrame,
        path: &Path,
        options: ExportOptions,
    ) -> Result<u64, ExportError> {
        let (mut session_state, plan) = df.into_parts();

        session_state
            .config_mut()
            .options_mut()
            .execution
            .minimum_parallel_output_files = 1;

        session_state
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 1;

        if let Some(partition_size) = options.records_per_file {
            session_state
                .config_mut()
                .options_mut()
                .execution
                .soft_max_rows_per_output_file = partition_size;
        };

        let export_df = DataFrame::new(session_state, plan)
            .repartition(Partitioning::RoundRobinBatch(1))
            .int_err()?;

        let path_str = path.as_os_str().to_str().unwrap();

        let result = match &options.format {
            ExportFormat::Parquet => {
                export_df
                    .write_parquet(path_str, DataFrameWriteOptions::new(), None)
                    .await
            }
            ExportFormat::Csv => {
                export_df
                    .write_csv(path_str, DataFrameWriteOptions::new(), None)
                    .await
            }
            ExportFormat::NdJson => {
                export_df
                    .write_json(path_str, DataFrameWriteOptions::new(), None)
                    .await
            }
        }?;

        self.records_written(&result)
    }
}
