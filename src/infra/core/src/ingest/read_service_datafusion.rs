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
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::{Dataset, EngineError, InvalidQueryError, ObjectStoreRegistry, OwnedFile};
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
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    dataset: Arc<dyn Dataset>,
    run_info_dir: PathBuf,
}

impl ReadServiceDatafusion {
    pub fn new(
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        dataset: Arc<dyn Dataset>,
        run_info_dir: PathBuf,
    ) -> Self {
        Self {
            object_store_registry,
            dataset,
            run_info_dir,
        }
    }

    fn new_session_context(&self) -> SessionContext {
        use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

        let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");

        let runtime_config = RuntimeConfig {
            object_store_registry: self.object_store_registry.clone().as_datafusion_registry(),
            ..RuntimeConfig::default()
        };

        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

        SessionContext::with_config_rt(config, runtime)
    }

    // TODO: Replace with DI
    fn get_reader_for(conf: &odf::ReadStep, operation_dir: &Path) -> Arc<dyn Reader> {
        let temp_path = operation_dir.join("reader.tmp");
        match conf {
            odf::ReadStep::Csv(_) => Arc::new(ReaderCsv {}),
            odf::ReadStep::Json(_) => Arc::new(ReaderJson::new(temp_path)),
            odf::ReadStep::NdJson(_) => Arc::new(ReaderNdJson {}),
            odf::ReadStep::JsonLines(_) => Arc::new(ReaderNdJson {}),
            odf::ReadStep::GeoJson(_) => Arc::new(ReaderGeoJson::new(temp_path)),
            odf::ReadStep::NdGeoJson(_) => Arc::new(ReaderNdGeoJson::new(temp_path)),
            odf::ReadStep::EsriShapefile(_) => Arc::new(ReaderEsriShapefile::new(temp_path)),
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

    async fn get_all_previous_data(
        &self,
        ctx: &SessionContext,
        prev_data_slices: &Vec<odf::Multihash>,
    ) -> Result<Option<DataFrame>, InternalError> {
        if prev_data_slices.is_empty() {
            return Ok(None);
        }

        let data_repo = self.dataset.as_data_repo();

        use futures::StreamExt;
        let prev_data_paths: Vec<_> = futures::stream::iter(prev_data_slices.iter().rev())
            .then(|hash| data_repo.get_internal_url(hash))
            .map(|url| url.to_string())
            .collect()
            .await;

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

        Ok(Some(df))
    }

    // TODO: This function currently ensures that all timestamps in the ouput are
    // represeted as `Timestamp(Millis, "UTC")` for compatibility with other engines
    // (e.g. Flink does not support event time with nanosecond precision).
    fn normalize_raw_result(&self, df: DataFrame) -> Result<DataFrame, EngineError> {
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        let utc_tz: Arc<str> = Arc::from("UTC");
        let mut select: Vec<Expr> = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let expr = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                    col(field.unqualified_column())
                }
                DataType::Timestamp(_, _) => {
                    noop = false;
                    cast(
                        col(field.unqualified_column()),
                        DataType::Timestamp(TimeUnit::Millisecond, Some(utc_tz.clone())),
                    )
                    .alias(field.name())
                }
                _ => col(field.unqualified_column()),
            };
            select.push(expr);
        }

        if noop {
            Ok(df)
        } else {
            let df = df.select(select).int_err()?;
            tracing::info!(schema = ?df.schema(), "Schema after timestamp normalization");
            Ok(df)
        }
    }

    async fn with_system_columns(
        &self,
        df: DataFrame,
        vocab: &odf::DatasetVocabularyResolved<'_>,
        system_time: DateTime<Utc>,
        source_event_time: Option<DateTime<Utc>>,
        start_offset: i64,
    ) -> Result<DataFrame, EngineError> {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr as expr;
        use datafusion::logical_expr::expr::WindowFunction;
        use datafusion::scalar::ScalarValue;

        let system_columns = [&vocab.offset_column, &vocab.system_time_column];
        for system_column in system_columns {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Err(InvalidQueryError::new(
                    format!(
                        "Transformed data contains a column that conflicts with the system column \
                         name, you should either rename the data column or configure the dataset \
                         vocabulary to use a different name: {}",
                        system_column
                    ),
                    Vec::new(),
                )
                .into());
            }
        }

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
        let df = df
            .with_column(
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
            )
            .int_err()?;

        let df = df
            .with_column(
                &vocab.offset_column,
                cast(
                    col(&vocab.offset_column as &str) + lit(start_offset - 1),
                    DataType::Int64,
                ),
            )
            .int_err()?;

        // System time
        let df = df
            .with_column(
                &vocab.system_time_column,
                Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(system_time.timestamp_millis()),
                    Some("UTC".into()),
                )),
            )
            .int_err()?;

        // Event time - validate or add from source event time if data doesn't contain
        // this colum
        let event_time_col = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().as_str() == vocab.event_time_column);

        let df = if let Some(event_time_col) = event_time_col {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 => {}
                DataType::Timestamp(_, None) => {
                    return Err(InvalidQueryError::new(
                        format!(
                            "Event time column '{}' should be adjusted to UTC, but local/naive \
                             timestamp found",
                            vocab.event_time_column
                        ),
                        Vec::new(),
                    )
                    .into());
                }
                DataType::Timestamp(_, Some(tz)) => match tz as &str {
                    "+00:00" | "UTC" => {}
                    tz => {
                        // TODO: Is this restriction necessary?
                        // Datafusion has very sane (metadata-only) approach to storing timezones.
                        // The fear currently is about compatibility with engines like Spark/Flink
                        // that might interpret it incorrectly. This has to be tested further.
                        return Err(InvalidQueryError::new(
                            format!(
                                "Event time column '{}' should be adjusted to UTC, but found: {}",
                                vocab.event_time_column, tz
                            ),
                            Vec::new(),
                        )
                        .into());
                    }
                },
                typ => {
                    return Err(InvalidQueryError::new(
                        format!(
                            "Event time column '{}' should be either Date or Timestamp, but \
                             found: {}",
                            vocab.event_time_column, typ
                        ),
                        Vec::new(),
                    )
                    .into());
                }
            }

            df
        } else {
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
            )
            .int_err()?
        };

        // Reorder columns for nice looks
        let mut full_columns = vec![
            vocab.offset_column.to_string(),
            vocab.system_time_column.to_string(),
            vocab.event_time_column.to_string(),
        ];
        full_columns.append(&mut raw_columns_wo_event_time);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str).int_err()?;
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

    // TODO: Externalize configuration
    fn get_write_properties(&self, vocab: &odf::DatasetVocabularyResolved<'_>) -> WriterProperties {
        // TODO: `offset` column is sorted integers so we could use delta encoding, but
        // Flink does not support it.
        // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
        WriterProperties::builder()
            .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_1_0)
            .set_created_by("kamu ingest datafusion".to_string())
            .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
            // system_time value will be the same for all rows in a batch
            .set_column_dictionary_enabled(vocab.system_time_column.as_ref().into(), true)
            .build()
    }

    async fn write_output(
        &self,
        path: PathBuf,
        ctx: &SessionContext,
        df: DataFrame,
        vocab: &odf::DatasetVocabularyResolved<'_>,
    ) -> Result<IngestResponse, IngestError> {
        use datafusion::arrow::array::{
            Date32Array,
            Date64Array,
            Int64Array,
            TimestampMillisecondArray,
        };

        // Write parquet
        df.write_parquet_single_file(&path, Some(self.get_write_properties(vocab)))
            .await
            .int_err()?;

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
        let stats = df
            .aggregate(
                vec![],
                vec![
                    min(col(vocab.offset_column.as_ref())),
                    max(col(vocab.offset_column.as_ref())),
                    // TODO: Add support for more watermark strategies
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

        // Event time is either Date or Timestamp(Millisecond, UTC)
        let event_time_arr = batches[0].column(2).as_any();
        let event_time_max = if let Some(event_time_arr) =
            event_time_arr.downcast_ref::<TimestampMillisecondArray>()
        {
            let event_time_max_millis = event_time_arr.value(0);
            Utc.timestamp_millis_opt(event_time_max_millis).unwrap()
        } else if let Some(event_time_arr) = event_time_arr.downcast_ref::<Date64Array>() {
            let naive_datetime = event_time_arr.value_as_datetime(0).unwrap();
            DateTime::from_utc(naive_datetime, Utc)
        } else if let Some(event_time_arr) = event_time_arr.downcast_ref::<Date32Array>() {
            let naive_datetime = event_time_arr.value_as_datetime(0).unwrap();
            DateTime::from_utc(naive_datetime, Utc)
        } else {
            return Err(format!(
                "Expected event time column to be Date64 or Timestamp(Millisecond, UTC), but got \
                 {}",
                batches[0].schema().field(2)
            )
            .int_err()
            .into());
        };

        Ok(IngestResponse {
            data_interval: Some(odf::OffsetInterval {
                start: offset_min,
                end: offset_max,
            }),
            output_watermark: Some(event_time_max),
            out_checkpoint: None,
            out_data: Some(OwnedFile::new(path)),
        })
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

        let ctx = self.new_session_context();

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

        // Normalize timestamps
        let df = self.normalize_raw_result(df)?;

        // Merge step
        let df = {
            // TODO: PERF: We could benefit from ingest checkpointing here
            let prev = self
                .get_all_previous_data(&ctx, &request.prev_data_slices)
                .await?;

            let merge_strategy = self.get_merge_strategy(&source.merge, &vocab);
            let df = merge_strategy.merge(prev, df)?;

            tracing::info!(schema = ?df.schema(), "Merge step output schema");
            df
        };

        // Prepare and write output
        let df = self
            .with_system_columns(
                df,
                &vocab,
                request.system_time,
                request.event_time,
                request.next_offset,
            )
            .await?;

        tracing::info!(schema = ?df.schema(), "Final output schema");

        let mut response = self.write_output(out_data_path, &ctx, df, &vocab).await?;

        // Don't unpdate watermark unless it progressed
        response.output_watermark = match (response.output_watermark, request.prev_watermark) {
            (Some(new), Some(old)) if new < old => Some(old),
            (None, old) => old,
            (new, _) => new,
        };

        Ok(response)
    }
}
