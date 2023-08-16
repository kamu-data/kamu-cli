// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::{Dataset, EngineError, InvalidQueryError, OwnedFile};
use kamu_data_utils::data::dataframe_ext::*;
use kamu_ingest_datafusion::*;
use opendatafabric as odf;

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
    dataset: Arc<dyn Dataset>,
    run_info_dir: PathBuf,
}

impl ReadServiceDatafusion {
    pub fn new(dataset: Arc<dyn Dataset>, run_info_dir: PathBuf) -> Self {
        Self {
            dataset,
            run_info_dir,
        }
    }

    // TODO: Replace with DI
    fn get_reader_for(conf: &odf::ReadStep, operation_dir: &Path) -> Arc<dyn Reader> {
        match conf {
            odf::ReadStep::Csv(_) => Arc::new(ReaderCsv {}),
            odf::ReadStep::NdJson(_) => Arc::new(ReaderNdJson {}),
            odf::ReadStep::JsonLines(_) => Arc::new(ReaderNdJson {}),
            odf::ReadStep::GeoJson(_) => Arc::new(ReaderGeoJson::new(operation_dir.to_path_buf())),
            odf::ReadStep::EsriShapefile(_) => {
                Arc::new(ReaderEsriShapefile::new(operation_dir.to_path_buf()))
            }
            odf::ReadStep::Parquet(_) => Arc::new(ReaderParquet {}),
        }
    }

    fn get_merge_strategy(
        &self,
        conf: &odf::MergeStrategy,
        vocab: &odf::DatasetVocabularyResolved<'_>,
    ) -> Arc<dyn MergeStrategy> {
        match conf {
            odf::MergeStrategy::Append => Arc::new(MergeStrategyAppend),
            odf::MergeStrategy::Ledger(conf) => {
                Arc::new(MergeStrategyLedger::new(conf.primary_key.clone()))
            }
            odf::MergeStrategy::Snapshot(cfg) => Arc::new(MergeStrategySnapshot::new(
                vocab.offset_column.to_string(),
                cfg.clone(),
            )),
        }
    }

    async fn with_system_columns(
        df: DataFrame,
        vocab: &odf::DatasetVocabularyResolved<'_>,
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
        Ok(df)
    }

    async fn register_view_for_step(
        &self,
        ctx: &SessionContext,
        step: &odf::SqlQueryStep,
    ) -> Result<(), EngineError> {
        use datafusion::logical_expr::*;
        use datafusion::sql::TableReference;

        let name = step.alias.as_ref().unwrap();

        tracing::info!(
            %name,
            query = %step.query,
            "Creating view for a query",
        );

        let logical_plan = match ctx.state().create_logical_plan(&step.query).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::debug!(?error, query = %step.query, "Error when setting up query");
                return Err(InvalidQueryError::new(error.to_string(), Vec::new()).into());
            }
        };

        let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: TableReference::bare(step.alias.as_ref().unwrap()).to_owned_reference(),
            input: Arc::new(logical_plan),
            or_replace: false,
            definition: Some(step.query.clone()),
        }));

        ctx.execute_logical_plan(create_view).await.int_err()?;
        Ok(())
    }

    async fn write_output(
        &self,
        path: PathBuf,
        ctx: &SessionContext,
        df: DataFrame,
        vocab: &odf::DatasetVocabularyResolved<'_>,
    ) -> Result<IngestResponse, IngestError> {
        use datafusion::arrow::array::{Int64Array, TimestampMillisecondArray};
        //use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        //use datafusion::parquet::file::statistics::Statistics;

        // Write parquet
        df.write_parquet_single_file(&path, None).await.int_err()?;

        // Read file back (metadata-only query) to get offsets and watermark
        let df = ctx
            .read_parquet(
                path.to_str().unwrap(),
                ParquetReadOptions {
                    file_extension: path.extension().unwrap_or_default().to_str().unwrap(),
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                },
            )
            .await
            .int_err()?;

        // Remove file if empty
        if df.clone().count().await.int_err()? == 0 {
            std::fs::remove_file(&path).int_err()?;

            return Ok(IngestResponse {
                data_interval: None,
                output_watermark: None,
                out_checkpoint: None,
                out_data: None,
            });
        }

        // Calculate watermark as max(event_time)
        // TODO: Add support for more watermark strategies
        let stats = df
            .aggregate(
                vec![],
                vec![
                    min(col(vocab.offset_column.as_ref())),
                    max(col(vocab.offset_column.as_ref())),
                    max(col(vocab.event_time_column.as_ref())),
                ],
            )
            .int_err()?;

        let batches = stats.collect().await.int_err()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let offset_min = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        let offset_max = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        let event_time_max_millis = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);

        let event_time_max = Utc.timestamp_millis_opt(event_time_max_millis).unwrap();

        Ok(IngestResponse {
            data_interval: Some(odf::OffsetInterval {
                start: offset_min,
                end: offset_max,
            }),
            output_watermark: Some(event_time_max),
            out_checkpoint: None,
            out_data: Some(OwnedFile::new(path)),
        })

        /*
        let reader = SerializedFileReader::new(std::fs::File::open(&path).int_err()?).int_err()?;
        let metadata = reader.metadata();
        let num_rows: i64 = reader.metadata().file_metadata().num_rows();

        // Print metadata for debugging
        let metadata_str = {
            let mut metadata_buf = Vec::new();
            datafusion::parquet::schema::printer::print_parquet_metadata(
                &mut metadata_buf,
                metadata,
            );
            String::from_utf8(metadata_buf).unwrap()
        };
        tracing::info!(
            metadata = %metadata_str,
            num_rows,
            "Wrote data to parquet file"
        );

        if num_rows == 0 {
            std::fs::remove_file(&path).int_err()?;
            Ok(IngestResponse {
                data_interval: None,
                output_watermark: None,
                out_checkpoint: None,
                out_data: None,
            })
        } else {
            let mut offset_min = i64::MAX;
            let mut offset_max = i64::MIN;
            for rg in metadata.row_groups() {
                let offset_stats = match rg.column(0).statistics() {
                    Some(Statistics::Int64(s)) => Ok(s),
                    s => Err(format!("Expected i64 offset statistics but got: {:?}", s).int_err()),
                }?;

                offset_min = offset_min.min(*offset_stats.min());
                offset_max = offset_max.min(*offset_stats.max());
            }
        }*/
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

        // Read step
        let df = {
            let reader = Self::get_reader_for(&source.read, &operation_dir);
            reader.read(&ctx, &in_data_path, &source.read).await?
        };

        tracing::info!(schema = ?df.schema(), "Read step output schema");

        // Preprocess step
        let df = if let Some(preprocess) = source.preprocess.clone() {
            let odf::Transform::Sql(preprocess) = preprocess;
            let preprocess = preprocess.normalize_queries(Some("output".to_string()));
            assert_eq!(preprocess.engine.to_lowercase(), "datafusion");

            // Setup input
            ctx.register_table("input", df.into_view()).int_err()?;

            // Setup queries
            for query_step in preprocess.queries.unwrap_or_default() {
                self.register_view_for_step(&ctx, &query_step).await?;
            }

            // Get result's execution plan
            let df = ctx.table("output").await.int_err()?;
            tracing::info!(schema = ?df.schema(), "Preprocess step output schema");
            df
        } else {
            df
        };

        // Merge step
        let df = {
            // Prepare previous data
            let prev = if request.prev_data_slices.is_empty() {
                None
            } else {
                // TODO: PERF: We could benefit from ingest checkpointing here
                let data_repo = self.dataset.as_data_repo();

                // Assuming slices are in reverse chronological order and we flip it to make the
                // datafusion's job of sorting by offset (as needed by snapshot strategy) easier
                use futures::{StreamExt, TryStreamExt};
                let prev_data_paths: Vec<_> =
                    futures::stream::iter(request.prev_data_slices.iter().rev())
                        .then(|hash| data_repo.get_internal_url(hash))
                        .map(|url| kamu_data_utils::data::local_url::into_local_path(url))
                        .map_ok(|path| path.to_string_lossy().into_owned())
                        .try_collect()
                        .await
                        .int_err()?;

                let df = ctx
                    .read_parquet(
                        prev_data_paths,
                        ParquetReadOptions {
                            file_extension: "",
                            table_partition_cols: Vec::new(),
                            parquet_pruning: None,
                            skip_metadata: None,
                        },
                    )
                    .await
                    .int_err()?;

                Some(df)
            };

            let merge_strategy = self.get_merge_strategy(&source.merge, &vocab);
            let df = merge_strategy.merge(prev, df)?;
            tracing::info!(schema = ?df.schema(), "Merge step output schema");
            df
        };

        // Prepare and write output
        let df = Self::with_system_columns(
            df,
            &vocab,
            request.system_time,
            request.event_time,
            request.next_offset,
        )
        .await
        .int_err()?;

        tracing::info!(schema = ?df.schema(), "Final output schema");

        self.write_output(out_data_path, &ctx, df, &vocab).await
    }
}
