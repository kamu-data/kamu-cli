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
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::*;
use kamu_core::*;
use odf::{AsTypedBlock, DatasetVocabulary, MergeStrategyAppend};
use opendatafabric as odf;

///////////////////////////////////////////////////////////////////////////////

/// Implementation of the [`DataWriter`] interface using Apache `DataFusion`
/// engine
pub struct DataWriterDataFusion {
    dataset: Arc<dyn Dataset>,
    merge_strategy: Arc<dyn MergeStrategy>,
    block_ref: BlockRef,

    // Mutable
    meta: DataWriterMetadataState,

    // Can be reset
    ctx: SessionContext,
}

/// Contains a projection of the metadata needed for [`DataWriter`] to function
#[derive(Debug, Clone)]
pub struct DataWriterMetadataState {
    pub head: odf::Multihash,
    pub schema: Option<SchemaRef>,
    pub source_event: Option<odf::MetadataEvent>,
    pub merge_strategy: odf::MergeStrategy,
    pub vocab: odf::DatasetVocabulary,
    pub data_slices: Vec<odf::Multihash>,
    pub prev_offset: Option<u64>,
    pub prev_checkpoint: Option<odf::Multihash>,
    pub prev_watermark: Option<DateTime<Utc>>,
    pub prev_source_state: Option<odf::SourceState>,
}

///////////////////////////////////////////////////////////////////////////////

impl DataWriterDataFusion {
    pub fn builder(dataset: Arc<dyn Dataset>, ctx: SessionContext) -> DataWriterDataFusionBuilder {
        DataWriterDataFusionBuilder::new(dataset, ctx)
    }

    /// Use [`Self::builder`] to create an instance
    fn new(
        ctx: SessionContext,
        dataset: Arc<dyn Dataset>,
        merge_strategy: Arc<dyn MergeStrategy>,
        block_ref: BlockRef,
        metadata_state: DataWriterMetadataState,
    ) -> Self {
        Self {
            ctx,
            dataset,
            merge_strategy,
            block_ref,
            meta: metadata_state,
        }
    }

    pub fn prev_offset(&self) -> Option<u64> {
        self.meta.prev_offset
    }

    pub fn prev_source_state(&self) -> Option<&odf::SourceState> {
        self.meta.prev_source_state.as_ref()
    }

    pub fn vocab(&self) -> &odf::DatasetVocabulary {
        &self.meta.vocab
    }

    pub fn source_event(&self) -> Option<&odf::MetadataEvent> {
        self.meta.source_event.as_ref()
    }

    pub fn set_session_context(&mut self, ctx: SessionContext) {
        self.ctx = ctx;
    }

    fn validate_input(&self, df: &DataFrame) -> Result<(), BadInputSchemaError> {
        for system_column in [
            &self.meta.vocab.offset_column,
            &self.meta.vocab.operation_type_column,
            &self.meta.vocab.system_time_column,
        ] {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Err(BadInputSchemaError::new(
                    format!(
                        "Data contains a column that conflicts with the system column name, you \
                         should either rename the data column or configure the dataset vocabulary \
                         to use a different name: {system_column}"
                    ),
                    SchemaRef::new(df.schema().into()),
                ));
            }
        }

        // Event time: If present must be a TIMESTAMP or DATE
        let event_time_col = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().as_str() == self.meta.vocab.event_time_column);

        if let Some(event_time_col) = event_time_col {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {}
                typ => {
                    return Err(BadInputSchemaError::new(
                        format!(
                            "Event time column '{}' should be either Date or Timestamp, but \
                             found: {}",
                            self.meta.vocab.event_time_column, typ
                        ),
                        SchemaRef::new(df.schema().into()),
                    ));
                }
            }
        }

        Ok(())
    }

    // TODO: This function currently ensures that all timestamps in the output are
    // represented as `Timestamp(Millis, "UTC")` for compatibility with other
    // engines (e.g. Flink does not support event time with nanosecond
    // precision).
    fn normalize_raw_result(&self, df: DataFrame) -> Result<DataFrame, InternalError> {
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
            tracing::debug!(schema = ?df.schema(), "Schema after timestamp normalization");
            Ok(df)
        }
    }

    /// Populates event time column with nulls if it does not exist
    fn ensure_event_time_column(
        &self,
        df: DataFrame,
        prev_schema: Option<&DFSchema>,
    ) -> Result<DataFrame, InternalError> {
        if !df
            .schema()
            .has_column_with_unqualified_name(&self.meta.vocab.event_time_column)
        {
            let data_type = prev_schema
                .and_then(|s| {
                    s.field_with_unqualified_name(&self.meta.vocab.event_time_column)
                        .ok()
                })
                .map_or(
                    DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
                    |f| f.data_type().clone(),
                );

            tracing::debug!("Event time column is missing - source fallback time will be used");
            df.with_column(
                &self.meta.vocab.event_time_column,
                cast(
                    Expr::Literal(datafusion::scalar::ScalarValue::Null),
                    data_type,
                ),
            )
            .int_err()
        } else {
            Ok(df)
        }
    }

    // TODO: PERF: This will not scale well as number of blocks grows
    async fn get_all_previous_data(
        &self,
        prev_data_slices: &[odf::Multihash],
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

        let df = self
            .ctx
            .read_parquet(
                prev_data_paths,
                ParquetReadOptions {
                    // TODO: Specify schema
                    schema: None,
                    file_extension: "",
                    // TODO: PERF: Possibly speed up by specifying `offset`
                    file_sort_order: Vec::new(),
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    insert_mode: datafusion::datasource::listing::ListingTableInsertMode::Error,
                },
            )
            .await
            .int_err()?;

        Ok(Some(df))
    }

    fn with_system_columns(
        &self,
        df: DataFrame,
        system_time: DateTime<Utc>,
        fallback_event_time: DateTime<Utc>,
        start_offset: u64,
    ) -> Result<DataFrame, InternalError> {
        use datafusion::logical_expr as expr;
        use datafusion::logical_expr::expr::WindowFunction;
        use datafusion::scalar::ScalarValue;

        // Collect column names for later
        let mut data_columns: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| {
                n.as_str() != self.meta.vocab.event_time_column
                    && n.as_str() != self.meta.vocab.operation_type_column
            })
            .collect();

        // System time
        let df = df
            .with_column(
                &self.meta.vocab.system_time_column,
                Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some(system_time.timestamp_millis()),
                    Some("UTC".into()),
                )),
            )
            .int_err()?;

        // Event time
        // If event time column is not present in the source data, after merge step it
        // may still have some null values. We need to fill those with the fallback
        // event time from the data source.
        let event_time_data_type = df
            .schema()
            .field_with_unqualified_name(&self.meta.vocab.event_time_column)
            .int_err()?
            .data_type()
            .clone();
        let df = df
            .with_column(
                &self.meta.vocab.event_time_column,
                // TODO: Using `case` expression instead of `coalesce()` due to DataFusion bug
                // See: https://github.com/apache/arrow-datafusion/issues/8790
                when(
                    col(&self.meta.vocab.event_time_column).is_null(),
                    cast(
                        Expr::Literal(ScalarValue::TimestampMillisecond(
                            Some(fallback_event_time.timestamp_millis()),
                            Some("UTC".into()),
                        )),
                        event_time_data_type,
                    ),
                )
                .otherwise(col(&self.meta.vocab.event_time_column))
                .int_err()?,
            )
            .int_err()?;

        // Offset & event time
        // Note: ODF expects events within one chunk to be sorted by event time, so we
        // ensure data is held in one partition to avoid reordering when saving to
        // parquet.
        // TODO: For some reason this adds two columns: the expected
        // "offset", but also "ROW_NUMBER()" for now we simply filter out the
        // latter.
        let df = df
            .repartition(Partitioning::RoundRobinBatch(1))
            .int_err()?
            .with_column(
                &self.meta.vocab.offset_column,
                Expr::WindowFunction(WindowFunction {
                    fun: expr::WindowFunction::BuiltInWindowFunction(
                        expr::BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: self.merge_strategy.sort_order(),
                    window_frame: expr::WindowFrame::new(false),
                }),
            )
            .int_err()?;

        let df = df
            .with_column(
                &self.meta.vocab.offset_column,
                cast(
                    col(&self.meta.vocab.offset_column as &str) + lit(start_offset as i64 - 1),
                    // TODO: Replace with UInt64 after Spark is updated
                    // See: https://github.com/kamu-data/kamu-cli/issues/445
                    DataType::Int64,
                ),
            )
            .int_err()?;

        // Reorder columns for nice looks
        let mut full_columns = vec![
            self.meta.vocab.offset_column.to_string(),
            self.meta.vocab.operation_type_column.to_string(),
            self.meta.vocab.system_time_column.to_string(),
            self.meta.vocab.event_time_column.to_string(),
        ];
        full_columns.append(&mut data_columns);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str).int_err()?;
        Ok(df)
    }

    pub fn validate_output_schema_equivalence(
        prev_schema: &SchemaRef,
        new_schema: &SchemaRef,
    ) -> Result<(), IncompatibleSchemaError> {
        if !Self::is_schema_equivalent(prev_schema, new_schema) {
            Err(IncompatibleSchemaError::new(
                "Schema of the new slice differs from the schema defined by SetDataSchema event",
                prev_schema.clone(),
                new_schema.clone(),
            ))
        } else {
            Ok(())
        }
    }

    fn is_schema_equivalent(lhs: &SchemaRef, rhs: &SchemaRef) -> bool {
        lhs.fields().len() == rhs.fields().len()
            && lhs
                .fields()
                .iter()
                .zip(rhs.fields().iter())
                .all(|(l, r)| Self::is_schema_equivalent_rec(l, r))
    }

    fn is_schema_equivalent_rec(lhs: &Field, rhs: &Field) -> bool {
        // Ignore nullability
        lhs.name() == rhs.name()
            && lhs.data_type() == rhs.data_type()
            && lhs.metadata() == rhs.metadata()
    }

    // TODO: Externalize configuration
    fn get_write_properties(&self) -> WriterProperties {
        // TODO: `offset` column is sorted integers so we could use delta encoding, but
        // Flink does not support it.
        // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
        WriterProperties::builder()
            .set_writer_version(datafusion::parquet::file::properties::WriterVersion::PARQUET_1_0)
            .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
            // op column is low cardinality and best encoded as RLE_DICTIONARY
            .set_column_dictionary_enabled(
                self.meta.vocab.operation_type_column.as_str().into(),
                true,
            )
            // system_time value will be the same for all rows in a batch
            .set_column_dictionary_enabled(self.meta.vocab.system_time_column.as_str().into(), true)
            .build()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?path))]
    async fn write_output(
        &self,
        path: PathBuf,
        df: DataFrame,
    ) -> Result<Option<OwnedFile>, InternalError> {
        use datafusion::arrow::array::UInt64Array;

        let res = df
            .write_parquet(
                path.as_os_str().to_str().unwrap(),
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(self.get_write_properties()),
            )
            .await
            .int_err()?;

        let file = OwnedFile::new(path);

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].num_columns(), 1);
        assert_eq!(res[0].num_rows(), 1);
        let num_records = res[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);

        if num_records > 0 {
            tracing::info!(
                path = ?file.as_path(),
                num_records,
                "Produced parquet file",
            );
            Ok(Some(file))
        } else {
            tracing::info!("Produced empty result",);
            Ok(None) // Empty file will be cleaned up here
        }
    }

    // Read output file back (metadata-only query) to get offsets and watermark
    async fn compute_offset_and_watermark(
        &self,
        path: &Path,
        prev_watermark: Option<DateTime<Utc>>,
    ) -> Result<(odf::OffsetInterval, Option<DateTime<Utc>>), InternalError> {
        use datafusion::arrow::array::*;

        let df = self
            .ctx
            .read_parquet(
                path.to_str().unwrap(),
                ParquetReadOptions {
                    schema: None,
                    file_sort_order: Vec::new(),
                    file_extension: path.extension().unwrap_or_default().to_str().unwrap(),
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    insert_mode: datafusion::datasource::listing::ListingTableInsertMode::Error,
                },
            )
            .await
            .int_err()?;

        // Data must not be empty
        assert_ne!(df.clone().count().await.int_err()?, 0);

        // Calculate stats
        let stats = df
            .aggregate(
                vec![],
                vec![
                    min(col(&self.meta.vocab.offset_column)),
                    max(col(&self.meta.vocab.offset_column)),
                    // TODO: Add support for more watermark strategies
                    max(col(&self.meta.vocab.event_time_column)),
                ],
            )
            .int_err()?;

        let batches = stats.collect().await.int_err()?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        // TODO: Replace with UInt64Array after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
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

        let offset_interval = odf::OffsetInterval {
            start: offset_min as u64,
            end: offset_max as u64,
        };

        // Event time is either Date or Timestamp(Millisecond, UTC)
        let event_time_arr = batches[0].column(2).as_any();
        let event_time_max = if let Some(event_time_arr) =
            event_time_arr.downcast_ref::<TimestampMillisecondArray>()
        {
            let event_time_max_millis = event_time_arr.value(0);
            Utc.timestamp_millis_opt(event_time_max_millis).unwrap()
        } else if let Some(event_time_arr) = event_time_arr.downcast_ref::<Date64Array>() {
            let naive_datetime = event_time_arr.value_as_datetime(0).unwrap();
            DateTime::from_naive_utc_and_offset(naive_datetime, Utc)
        } else if let Some(event_time_arr) = event_time_arr.downcast_ref::<Date32Array>() {
            let naive_datetime = event_time_arr.value_as_datetime(0).unwrap();
            DateTime::from_naive_utc_and_offset(naive_datetime, Utc)
        } else {
            return Err(format!(
                "Expected event time column to be Date64 or Timestamp(Millisecond, UTC), but got \
                 {}",
                batches[0].schema().field(2)
            )
            .int_err());
        };

        // Ensure watermark is monotonically non-decreasing
        let output_watermark = match prev_watermark {
            None => Some(event_time_max),
            Some(prev) if prev < event_time_max => Some(event_time_max),
            prev => prev,
        };

        Ok((offset_interval, output_watermark))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DataWriter for DataWriterDataFusion {
    #[tracing::instrument(level = "info", skip_all)]
    async fn write(
        &mut self,
        new_data: Option<DataFrame>,
        opts: WriteDataOpts,
    ) -> Result<WriteDataResult, WriteDataError> {
        let staged = self.stage(new_data, opts).await?;
        let commit = self.commit(staged).await?;
        Ok(commit)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn write_watermark(
        &mut self,
        new_watermark: DateTime<Utc>,
        opts: WriteWatermarkOpts,
    ) -> Result<WriteDataResult, WriteWatermarkError> {
        // Do we have anything to commit?
        if Some(&new_watermark) == self.meta.prev_watermark.as_ref()
            && opts.new_source_state == self.meta.prev_source_state
        {
            return Err(EmptyCommitError {}.into());
        }

        let add_data = AddDataParams {
            prev_checkpoint: self.meta.prev_checkpoint.clone(),
            prev_offset: self.meta.prev_offset,
            new_offset_interval: None,
            new_watermark: Some(new_watermark),
            new_source_state: opts.new_source_state,
        };

        let staged = StageDataResult {
            system_time: opts.system_time,
            add_data,
            new_schema: None,
            data_file: None,
        };

        let commit = self.commit(staged).await?;
        Ok(commit)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn stage(
        &self,
        new_data: Option<DataFrame>,
        opts: WriteDataOpts,
    ) -> Result<StageDataResult, StageDataError> {
        let (add_data, new_schema, data_file) = if let Some(new_data) = new_data {
            self.validate_input(&new_data)?;

            // Normalize timestamps
            let df = self.normalize_raw_result(new_data)?;

            // Merge step
            // TODO: PERF: We could likely benefit from checkpointing here
            let prev = self.get_all_previous_data(&self.meta.data_slices).await?;

            // Populate event time with nulls if missing, using matching type to prev data
            let df = self.ensure_event_time_column(df, prev.as_ref().map(DataFrame::schema))?;

            let df = self.merge_strategy.merge(prev, df)?;

            tracing::debug!(
                schema = ?df.schema(),
                logical_plan = ?df.logical_plan(),
                "Performing merge step",
            );

            // Add system columns
            let df = self.with_system_columns(
                df,
                opts.system_time,
                opts.source_event_time,
                self.meta.prev_offset.map_or(0, |e| e + 1),
            )?;

            // Validate schema matches the declared one
            let new_schema = SchemaRef::new(df.schema().into());
            tracing::info!(schema = ?new_schema, "Final output schema");

            if let Some(prev_schema) = &self.meta.schema {
                Self::validate_output_schema_equivalence(prev_schema, &new_schema)?;
            }

            // Write output
            let data_file = self.write_output(opts.data_staging_path, df).await?;

            // Prepare commit info
            let prev_offset = self.meta.prev_offset;
            let prev_checkpoint = self.meta.prev_checkpoint.clone();
            let new_source_state = opts.new_source_state;
            let prev_watermark = self.meta.prev_watermark;

            if data_file.is_none() {
                // Empty result - carry watermark and propagate source state
                (
                    AddDataParams {
                        prev_checkpoint,
                        prev_offset,
                        new_offset_interval: None,
                        new_watermark: opts.new_watermark.or(prev_watermark),
                        new_source_state,
                    },
                    Some(new_schema),
                    None,
                )
            } else {
                let (new_offset_interval, new_watermark_from_data) = self
                    .compute_offset_and_watermark(
                        data_file.as_ref().unwrap().as_path(),
                        prev_watermark,
                    )
                    .await?;

                (
                    AddDataParams {
                        prev_checkpoint,
                        prev_offset,
                        new_offset_interval: Some(new_offset_interval),
                        new_watermark: opts.new_watermark.or(new_watermark_from_data),
                        new_source_state,
                    },
                    Some(new_schema),
                    data_file,
                )
            }
        } else {
            // TODO: Should watermark be advanced by the source event time?
            let add_data = AddDataParams {
                prev_checkpoint: self.meta.prev_checkpoint.clone(),
                prev_offset: self.meta.prev_offset,
                new_offset_interval: None,
                new_watermark: self.meta.prev_watermark,
                new_source_state: opts.new_source_state,
            };

            (add_data, None, None)
        };

        // Do we have anything to commit?
        if add_data.new_offset_interval.is_none()
            && add_data.new_watermark == self.meta.prev_watermark
            && add_data.new_source_state == self.meta.prev_source_state
        {
            Err(EmptyCommitError {}.into())
        } else {
            Ok(StageDataResult {
                system_time: opts.system_time,
                add_data,
                new_schema,
                data_file,
            })
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn commit(&mut self, staged: StageDataResult) -> Result<WriteDataResult, CommitError> {
        let old_head = self.meta.head.clone();

        // Commit schema if it was not previously defined
        if self.meta.schema.is_none() {
            if let Some(new_schema) = staged.new_schema {
                // TODO: Make commit of schema and data atomic
                let commit_schema_result = self
                    .dataset
                    .commit_event(
                        odf::SetDataSchema::new(&new_schema).into(),
                        CommitOpts {
                            block_ref: &self.block_ref,
                            system_time: Some(staged.system_time),
                            prev_block_hash: Some(Some(&self.meta.head)),
                            check_object_refs: false,
                        },
                    )
                    .await?;

                // Update state
                self.meta.head = commit_schema_result.new_head;
                self.meta.schema = Some(new_schema);
            }
        }

        let commit_data_result = self
            .dataset
            .commit_add_data(
                staged.add_data,
                staged.data_file,
                None,
                CommitOpts {
                    block_ref: &self.block_ref,
                    system_time: Some(staged.system_time),
                    prev_block_hash: Some(Some(&self.meta.head)),
                    check_object_refs: false,
                },
            )
            .await?;

        // Update state for the next append
        let new_block = self
            .dataset
            .as_metadata_chain()
            .get_block(&commit_data_result.new_head)
            .await
            .int_err()?
            .into_typed::<odf::AddData>()
            .unwrap();

        self.meta.head = commit_data_result.new_head.clone();

        if let Some(new_data) = &new_block.event.new_data {
            self.meta.prev_offset = Some(new_data.offset_interval.end);
            self.meta.data_slices.push(new_data.physical_hash.clone());
        }

        self.meta.prev_checkpoint = new_block
            .event
            .new_checkpoint
            .as_ref()
            .map(|c| c.physical_hash.clone());

        self.meta.prev_watermark = new_block.event.new_watermark;
        self.meta.prev_source_state = new_block.event.new_source_state.clone();

        Ok(WriteDataResult {
            old_head,
            new_head: commit_data_result.new_head,
            new_block,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////
// Builder
///////////////////////////////////////////////////////////////////////////////

pub struct DataWriterDataFusionBuilder {
    dataset: Arc<dyn Dataset>,
    ctx: SessionContext,
    block_ref: BlockRef,
    metadata_state: Option<DataWriterMetadataState>,
}

impl DataWriterDataFusionBuilder {
    pub fn new(dataset: Arc<dyn Dataset>, ctx: SessionContext) -> Self {
        Self {
            dataset,
            ctx,
            block_ref: BlockRef::Head,
            metadata_state: None,
        }
    }

    pub fn with_block_ref(self, block_ref: BlockRef) -> Self {
        Self { block_ref, ..self }
    }

    pub fn metadata_state(&self) -> Option<&DataWriterMetadataState> {
        self.metadata_state.as_ref()
    }

    /// Use to specify all needed state for builder to avoid scanning the
    /// metadata chain
    pub fn with_metadata_state(self, metadata_state: DataWriterMetadataState) -> Self {
        Self {
            metadata_state: Some(metadata_state),
            ..self
        }
    }

    /// Scans metadata chain to populate the needed metadata
    ///
    /// * `source_name` - name of the source to use when extracting the metadata
    ///   needed for writing. Leave empty for polling sources or to use the only
    ///   push source defined when there is no ambiguity.
    pub async fn with_metadata_state_scanned(
        self,
        source_name: Option<&str>,
    ) -> Result<Self, ScanMetadataError> {
        // TODO: PERF: Full metadata scan below - this is expensive and should be
        // improved using skip lists and caching.

        let head = self
            .dataset
            .as_metadata_chain()
            .get_ref(&self.block_ref)
            .await
            .int_err()?;

        let mut schema = None;
        let mut source_event: Option<odf::MetadataEvent> = None;
        let mut set_vocab = None;
        let mut data_slices = Vec::new();
        let mut prev_checkpoint = None;
        let mut prev_watermark = None;
        let mut prev_source_state = None;
        let mut prev_offset = None;

        {
            use futures::stream::TryStreamExt;
            let mut block_stream = self
                .dataset
                .as_metadata_chain()
                .iter_blocks_interval(&head, None, false);

            while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                match block.event {
                    odf::MetadataEvent::SetDataSchema(set_data_schema) => {
                        if schema.is_none() {
                            schema = Some(set_data_schema.schema_as_arrow().int_err()?);
                        }
                    }
                    odf::MetadataEvent::AddData(e) => {
                        if let Some(output_data) = &e.new_data {
                            data_slices.push(output_data.physical_hash.clone());
                        }
                        if prev_offset.is_none() {
                            prev_offset = Some(e.last_offset());
                        }
                        if prev_checkpoint.is_none() {
                            prev_checkpoint = Some(e.new_checkpoint.map(|cp| cp.physical_hash));
                        }
                        if prev_watermark.is_none() {
                            prev_watermark = Some(e.new_watermark);
                        }
                        if prev_source_state.is_none() {
                            if let Some(ss) = &e.new_source_state {
                                if source_name.is_some()
                                    && source_name != Some(ss.source_name.as_str())
                                {
                                    unimplemented!(
                                        "Differentiating between the state of multiple sources is \
                                         not yet supported"
                                    );
                                }
                            }
                            prev_source_state = Some(e.new_source_state);
                        }
                    }
                    odf::MetadataEvent::SetPollingSource(e) => {
                        if source_name.is_some() {
                            return Err(SourceNotFoundError::new(
                                source_name,
                                "Expected a named push source, but found polling source",
                            )
                            .into());
                        }
                        if source_event.is_none() {
                            source_event = Some(e.into());
                        }
                    }
                    odf::MetadataEvent::DisablePollingSource(_) => {
                        unimplemented!("Disabling sources is not yet fully supported")
                    }
                    odf::MetadataEvent::AddPushSource(e) => {
                        if source_event.is_none() {
                            if source_name.is_none() || source_name == Some(e.source_name.as_str())
                            {
                                source_event = Some(e.into());
                            }
                        } else {
                            // Encountered another source - if `source_name` was not specified we
                            // return ambiguity error
                            if source_name.is_none() {
                                return Err(SourceNotFoundError::new(
                                    None::<&str>,
                                    "Explicit source name is required to pick between several \
                                     active push sources",
                                )
                                .into());
                            }
                        }
                    }
                    odf::MetadataEvent::DisablePushSource(_) => {
                        unimplemented!("Disabling sources is not yet fully supported")
                    }
                    odf::MetadataEvent::SetVocab(e) => {
                        if set_vocab.is_none() {
                            set_vocab = Some(e);
                        }
                    }
                    odf::MetadataEvent::Seed(e) => {
                        assert_eq!(e.dataset_kind, odf::DatasetKind::Root);
                    }
                    odf::MetadataEvent::ExecuteTransform(_) => unreachable!(),
                    odf::MetadataEvent::SetAttachments(_)
                    | odf::MetadataEvent::SetInfo(_)
                    | odf::MetadataEvent::SetLicense(_)
                    | odf::MetadataEvent::SetTransform(_) => (),
                }
            }
        }

        let merge_strategy = match (&source_event, source_name) {
            // Source found
            (Some(e), _) => match e {
                odf::MetadataEvent::SetPollingSource(e) => Ok(e.merge.clone()),
                odf::MetadataEvent::AddPushSource(e) => Ok(e.merge.clone()),
                _ => unreachable!(),
            },
            // No source defined - assuming append strategy
            (None, None) => Ok(odf::MergeStrategy::Append(MergeStrategyAppend {})),
            // Source expected but not found
            (None, Some(source)) => Err(SourceNotFoundError::new(
                Some(source),
                format!("Source '{source}' not found"),
            )),
        }?;

        Ok(self.with_metadata_state(DataWriterMetadataState {
            head,
            schema,
            source_event,
            merge_strategy,
            vocab: set_vocab.unwrap_or_default().into(),
            data_slices,
            prev_offset: prev_offset.unwrap_or_default(),
            prev_checkpoint: prev_checkpoint.unwrap_or_default(),
            prev_watermark: prev_watermark.unwrap_or_default(),
            prev_source_state: prev_source_state.unwrap_or_default(),
        }))
    }

    pub fn build(self) -> DataWriterDataFusion {
        let Some(metadata_state) = self.metadata_state else {
            // TODO: Typestate
            panic!(
                "Writer state is undefined - use with_metadata_state_scanned() to initialize it \
                 from metadata chain or pass it explicitly via with_metadata_state()"
            )
        };

        let merge_strategy =
            Self::merge_strategy_for(metadata_state.merge_strategy.clone(), &metadata_state.vocab);

        DataWriterDataFusion::new(
            self.ctx,
            self.dataset,
            merge_strategy,
            self.block_ref,
            metadata_state,
        )
    }

    fn merge_strategy_for(
        conf: odf::MergeStrategy,
        vocab: &DatasetVocabulary,
    ) -> Arc<dyn MergeStrategy> {
        use crate::merge_strategies::*;

        match conf {
            odf::MergeStrategy::Append(_cfg) => Arc::new(MergeStrategyAppend::new(vocab.clone())),
            odf::MergeStrategy::Ledger(cfg) => {
                Arc::new(MergeStrategyLedger::new(vocab.clone(), cfg))
            }
            odf::MergeStrategy::Snapshot(cfg) => {
                Arc::new(MergeStrategySnapshot::new(vocab.clone(), cfg))
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ScanMetadataError {
    #[error(transparent)]
    SourceNotFound(
        #[from]
        #[backtrace]
        SourceNotFoundError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SourceNotFoundError {
    pub source_name: Option<String>,
    message: String,
}

impl SourceNotFoundError {
    pub fn new(source_name: Option<impl Into<String>>, message: impl Into<String>) -> Self {
        Self {
            source_name: source_name.map(std::convert::Into::into),
            message: message.into(),
        }
    }
}

impl From<SourceNotFoundError> for PushSourceNotFoundError {
    fn from(val: SourceNotFoundError) -> Self {
        PushSourceNotFoundError::new(val.source_name)
    }
}
