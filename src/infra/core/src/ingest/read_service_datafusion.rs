// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::OwnedFile;
use kamu_data_utils::data::dataframe_ext::*;
use kamu_ingest_datafusion::*;
use opendatafabric::*;

use crate::domain::{IngestError, IngestRequest, IngestResponse};

// TODO: ReadService is a temporary abstraction used while we are migrating from
// Spark to DataFusion.
//
// In future we will cleanly separate read, preprocess, and
// merge steps. The read step will be replaced by DataFusion's csv/json/parquet
// readers and readers for specialized formats (e.g. geojson). The preprocess
// step will default to DataFusion but it will still be possible to use Spark or
// other engines. The merge step will be all done via DataFusion.
#[async_trait::async_trait]
pub trait ReadService: Send + Sync {
    async fn read(&self, request: IngestRequest) -> Result<IngestResponse, IngestError>;
}

pub struct ReadServiceDatafusion {
    run_info_dir: PathBuf,
}

impl ReadServiceDatafusion {
    pub fn new(run_info_dir: PathBuf) -> Self {
        Self { run_info_dir }
    }

    // TODO: Replace with DI
    fn get_reader_for(conf: &ReadStep) -> Arc<dyn Reader> {
        match conf {
            ReadStep::Csv(_) => Arc::new(ReaderCsv {}),
            ReadStep::JsonLines(_) => Arc::new(ReaderNdJson {}),
            ReadStep::GeoJson(_) => todo!(),
            ReadStep::EsriShapefile(_) => todo!(),
            ReadStep::Parquet(_) => Arc::new(ReaderParquet {}),
        }
    }

    async fn with_system_columns(
        df: DataFrame,
        vocab: &DatasetVocabularyResolved<'_>,
        system_time: DateTime<Utc>,
        source_event_time: Option<DateTime<Utc>>,
        start_offset: i64,
    ) -> Result<DataFrame, DataFusionError> {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr as expr;
        use datafusion::logical_expr::expr::WindowFunction;
        use datafusion::scalar::ScalarValue;

        // Collect non-system column names for later
        let mut raw_columns_wo_event_time: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| n.as_str() != vocab.event_time_column)
            .collect();

        // Offset
        // TODO: For some reason this adds two collumns: the expected "offset", but also
        // "ROW_NUMBER()" for now we simply filter out the latter.
        let df = df.with_column(
            &vocab.offset_column,
            Expr::WindowFunction(WindowFunction {
                fun: expr::WindowFunction::BuiltInWindowFunction(
                    expr::BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: expr::WindowFrame::new(false),
            }),
        )?;

        let df = df.with_column(
            &vocab.offset_column,
            cast(
                col(&vocab.offset_column as &str) + lit(start_offset - 1),
                DataType::Int64,
            ),
        )?;

        // System time
        let df = df.with_column(
            &vocab.system_time_column,
            Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(system_time.timestamp_millis()),
                Some("UTC".into()),
            )),
        )?;

        // Event time (only if raw data doesn't already have this column)
        let df = if !df
            .schema()
            .has_column_with_unqualified_name(&vocab.event_time_column)
        {
            df.with_column(
                &vocab.event_time_column,
                if let Some(event_time) = source_event_time {
                    Expr::Literal(ScalarValue::TimestampMillisecond(
                        Some(event_time.timestamp_millis()),
                        Some("UTC".into()),
                    ))
                } else {
                    col(vocab.system_time_column.as_ref())
                },
            )?
        } else {
            df
        };

        // Reorder columns for nice looks
        let mut full_columns = vec![
            vocab.offset_column.to_string(),
            vocab.system_time_column.to_string(),
            vocab.event_time_column.to_string(),
        ];
        full_columns.append(&mut raw_columns_wo_event_time);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str)?;

        tracing::info!(schema = ?df.schema(), "Computed final result schema");
        Ok(df)
    }
}

#[async_trait::async_trait]
impl ReadService for ReadServiceDatafusion {
    async fn read(&self, request: IngestRequest) -> Result<IngestResponse, IngestError> {
        let operation_dir = self
            .run_info_dir
            .join(format!("ingest-{}", &request.operation_id));
        std::fs::create_dir_all(&operation_dir).int_err()?;

        let out_dir = operation_dir.join("out");
        std::fs::create_dir(&out_dir).int_err()?;

        let in_data_path = request.input_data_path;
        let out_data_path = out_dir.join("data");

        let vocab = request.vocab.into_resolved();
        let source = request.polling_source.unwrap();

        let ctx = SessionContext::new();

        let reader = Self::get_reader_for(&source.read);
        let df = reader
            .read(&ctx, &in_data_path, &source.read)
            .await
            .int_err()?; // TODO: error handling

        let df = Self::with_system_columns(
            df,
            &vocab,
            request.system_time,
            request.event_time,
            request.next_offset,
        )
        .await
        .int_err()?;

        df.write_parquet_single_file(&out_data_path, None)
            .await
            .int_err()?;

        let response = IngestResponse {
            data_interval: Some(OffsetInterval { start: 0, end: 0 }),
            output_watermark: None,
            out_checkpoint: None,
            out_data: Some(OwnedFile::new(out_data_path)),
        };

        Ok(response)
    }
}
