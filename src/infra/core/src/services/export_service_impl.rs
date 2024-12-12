// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, PrimitiveArray, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::UInt64Type;
use datafusion::dataframe::DataFrameWriteOptions;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{ExportFormat, ExportService, QueryService, SessionConfigOverrides};

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

    fn records_written(&self, batches: &Vec<RecordBatch>) -> Result<u64, InternalError> {
        let mut total = 0;
        for batch in batches {
            let col = batch
                .column_by_name("count")
                .ok_or("cannot get count col")
                .int_err()?; //todo: error
            let data: &PrimitiveArray<UInt64Type> = col
                .as_primitive_opt::<UInt64Type>()
                .ok_or("cannot cast count col data")
                .int_err()?; //todo: error
            let count = data
                .values()
                .get(0)
                .ok_or("cannot get count value")
                .int_err()?; //todo: error;
            total += count;
        }
        Ok(total)
    }
}

#[async_trait::async_trait]
impl ExportService for ExportServiceImpl {
    async fn export_to_fs(
        &self,
        sql_query: &String,
        path: &String,
        format: ExportFormat,
        partition_row_count: Option<usize>,
    ) -> Result<u64, InternalError> {
        // todo switch to specific error
        let config_overrides = SessionConfigOverrides {
            target_partitions: Some(1),
            soft_max_rows_per_output_file: partition_row_count,
            minimum_parallel_output_files: Some(1),
        };

        let ctx = self
            .query_service
            .create_session_with_config_overrides(config_overrides)
            .await
            .unwrap(); //todo

        let df = ctx.sql(sql_query).await.unwrap(); // todo: remove unwrap

        let result = match format {
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
        .int_err();

        result.and_then(|result| self.records_written(&result))
    }
}
