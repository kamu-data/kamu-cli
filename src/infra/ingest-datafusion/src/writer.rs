// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::config::{ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::functions_aggregate::min_max::{max, min};
use datafusion::prelude::*;
use file_utils::OwnedFile;
use internal_error::*;
use kamu_core::ingest::*;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementation of the [`DataWriter`] interface using Apache `DataFusion`
/// engine
pub struct DataWriterDataFusion {
    target: ResolvedDataset,
    merge_strategy: Arc<dyn MergeStrategy>,

    // Whether we should attempt to coerce schema of the new slice to match dataset schema, or
    // reject upon mismatch
    coerce_schema: bool,

    // Mutable
    meta: DataWriterMetadataState,

    // Can be reset
    ctx: SessionContext,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DataWriterDataFusion {
    pub async fn from_metadata_chain(
        ctx: SessionContext,
        target: ResolvedDataset,
        block_ref: &odf::BlockRef,
        source_name: Option<&str>,
    ) -> Result<Self, ScanMetadataError> {
        let metadata_state =
            DataWriterMetadataState::build(target.clone(), block_ref, source_name, None).await?;

        Ok(Self::from_metadata_state(ctx, target, metadata_state))
    }

    pub fn from_metadata_state(
        ctx: SessionContext,
        target: ResolvedDataset,
        metadata_state: DataWriterMetadataState,
    ) -> Self {
        let merge_strategy =
            Self::merge_strategy_for(metadata_state.merge_strategy.clone(), &metadata_state.vocab);

        Self {
            ctx,
            target,
            merge_strategy,
            meta: metadata_state,
            coerce_schema: true,
        }
    }

    pub fn as_metadata_state(&self) -> DataWriterMetadataState {
        self.meta.clone()
    }

    pub fn prev_offset(&self) -> Option<u64> {
        self.meta.prev_offset
    }

    pub fn prev_source_state(&self) -> Option<&odf::metadata::SourceState> {
        self.meta.prev_source_state.as_ref()
    }

    pub fn vocab(&self) -> &odf::metadata::DatasetVocabulary {
        &self.meta.vocab
    }

    pub fn source_event(&self) -> Option<&odf::MetadataEvent> {
        self.meta.source_event.as_ref()
    }

    pub fn set_session_context(&mut self, ctx: SessionContext) {
        self.ctx = ctx;
    }

    fn validate_input(&self, df: &DataFrameExt) -> Result<(), BadInputSchemaError> {
        let mut system_columns = vec![
            &self.meta.vocab.offset_column,
            &self.meta.vocab.system_time_column,
        ];

        match &self.meta.merge_strategy {
            odf::metadata::MergeStrategy::Append(_)
            | odf::metadata::MergeStrategy::Ledger(_)
            | odf::metadata::MergeStrategy::Snapshot(_) => {
                system_columns.push(&self.meta.vocab.operation_type_column);
            }
            odf::metadata::MergeStrategy::ChangelogStream(_)
            | odf::metadata::MergeStrategy::UpsertStream(_) => (),
        }

        for system_column in system_columns {
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
    fn normalize_raw_result(&self, df: DataFrameExt) -> Result<DataFrameExt, InternalError> {
        let utc_tz: Arc<str> = Arc::from("UTC");
        let mut select: Vec<Expr> = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let expr = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                    col(Column::from_name(field.name()))
                }
                DataType::Timestamp(_, _) => {
                    noop = false;
                    cast(
                        col(Column::from_name(field.name())),
                        DataType::Timestamp(TimeUnit::Millisecond, Some(utc_tz.clone())),
                    )
                    .alias(field.name())
                }
                _ => col(Column::from_name(field.name())),
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
        df: DataFrameExt,
        prev_schema: Option<&DFSchema>,
    ) -> Result<DataFrameExt, InternalError> {
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
                    Expr::Literal(datafusion::scalar::ScalarValue::Null, None),
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
        dataset_schema: Option<&SchemaRef>,
    ) -> Result<Option<DataFrameExt>, InternalError> {
        if prev_data_slices.is_empty() {
            return Ok(None);
        }

        let data_repo = self.target.as_data_repo();

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
                    schema: dataset_schema.map(Arc::as_ref),
                    file_extension: "",
                    // TODO: PERF: Possibly speed up by specifying `offset`
                    file_sort_order: Vec::new(),
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    file_decryption_properties: None,
                },
            )
            .await
            .int_err()?;

        Ok(Some(df.into()))
    }

    fn with_system_columns(
        &self,
        df: DataFrameExt,
        system_time: DateTime<Utc>,
        fallback_event_time: DateTime<Utc>,
        start_offset: u64,
    ) -> Result<DataFrameExt, InternalError> {
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
                Expr::Literal(
                    ScalarValue::TimestampMillisecond(
                        Some(system_time.timestamp_millis()),
                        Some("UTC".into()),
                    ),
                    None,
                ),
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
                datafusion::functions::core::coalesce().call(vec![
                    col(Column::from_name(&self.meta.vocab.event_time_column)),
                    cast(
                        Expr::Literal(
                            ScalarValue::TimestampMillisecond(
                                Some(fallback_event_time.timestamp_millis()),
                                Some("UTC".into()),
                            ),
                            None,
                        ),
                        event_time_data_type,
                    ),
                ]),
            )
            .int_err()?;

        // Assign offset based on the merge strategy's sort order
        // TODO: For some reason this adds two columns: the expected
        // "offset", but also "ROW_NUMBER()" for now we simply filter out the
        // latter.
        let df = df
            .with_column(
                &self.meta.vocab.offset_column,
                datafusion::functions_window::row_number::row_number()
                    .order_by(self.merge_strategy.sort_order())
                    .partition_by(vec![lit(1)])
                    .build()
                    .int_err()?,
            )
            .int_err()?;

        let df = df
            .with_column(
                &self.meta.vocab.offset_column,
                cast(
                    col(Column::from_name(&self.meta.vocab.offset_column))
                        + lit(i64::try_from(start_offset).unwrap() - 1),
                    // TODO: Replace with UInt64 after Spark is updated
                    // See: https://github.com/kamu-data/kamu-cli/issues/445
                    DataType::Int64,
                ),
            )
            .int_err()?;

        // Reorder columns for nice looks
        let mut full_columns = vec![
            self.meta.vocab.offset_column.clone(),
            self.meta.vocab.operation_type_column.clone(),
            self.meta.vocab.system_time_column.clone(),
            self.meta.vocab.event_time_column.clone(),
        ];
        full_columns.append(&mut data_columns);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str).int_err()?;

        // Note: As the very last step we sort the data by offset to guarantee its
        // sequential layout in the parquet file
        let df = df
            .sort(vec![col(&self.meta.vocab.offset_column).sort(true, true)])
            .int_err()?;

        Ok(df)
    }

    fn coerce_schema(&self, df: DataFrameExt) -> Result<DataFrameExt, InternalError> {
        if !self.coerce_schema {
            return Ok(df);
        }
        let Some(orig_schema) = &self.meta.schema else {
            return Ok(df);
        };

        // Currently only handling nullability coercion
        let orig_non_null_columns: std::collections::HashSet<String> = orig_schema
            .fields
            .iter()
            .filter(|f| !f.is_optional())
            .map(|f| f.name.clone())
            .collect();

        let df = df
            .assert_collumns_not_null(|f| orig_non_null_columns.contains(f.name()))
            .int_err()?;

        Ok(df)
    }

    // TODO: Replace this method with ODF schema comparator
    // Rules:
    // - New columns are allowed to be non-null if original types are nullable -
    //   they will be coerced on read
    // - Treat "large" variants equivalent to regular variants
    // - Treat view types equivalent to regular types
    pub fn validate_schema_compatible(
        orig_schema: &SchemaRef,
        new_schema: &SchemaRef,
    ) -> Result<(), IncompatibleSchemaError> {
        if !Self::is_schema_compatible(orig_schema.fields(), new_schema.fields()) {
            Err(IncompatibleSchemaError::new(
                "Schema of the new slice is not compatible with the schema defined by \
                 SetDataSchema event",
                orig_schema.clone(),
                new_schema.clone(),
            ))
        } else {
            Ok(())
        }
    }

    fn is_schema_compatible(orig: &Fields, new: &Fields) -> bool {
        orig.len() == new.len()
            && orig
                .iter()
                .zip(new.iter())
                .all(|(o, n)| Self::is_schema_compatible_rec(o, n))
    }

    fn is_schema_compatible_rec(orig: &Field, new: &Field) -> bool {
        if orig.name() != new.name()
            || orig.metadata() != new.metadata()
            || (!orig.is_nullable() && new.is_nullable())
        {
            return false;
        }

        // Avoid wildcard matching - we want compiler to force us re-check our
        // assumptions whenever new type is added
        match (orig.data_type(), new.data_type()) {
            (
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::FixedSizeBinary(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Union(_, _)
                | DataType::Dictionary(_, _)
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Map(_, _)
                | DataType::RunEndEncoded(_, _),
                _,
            ) => orig.data_type() == new.data_type(),
            (
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            )
            | (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            ) => true,
            (
                DataType::List(l)
                | DataType::ListView(l)
                | DataType::LargeList(l)
                | DataType::LargeListView(l),
                DataType::List(r)
                | DataType::ListView(r)
                | DataType::LargeList(r)
                | DataType::LargeListView(r),
            ) => Self::is_schema_compatible_rec(l, r),
            (DataType::Struct(l), DataType::Struct(r)) => Self::is_schema_compatible(l, r),
            (
                DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::List(_)
                | DataType::ListView(_)
                | DataType::LargeList(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_),
                _,
            ) => false,
        }
    }

    // TODO: Externalize configuration
    fn get_write_properties(&self) -> TableParquetOptions {
        // TODO: `offset` column is sorted integers so we could use delta encoding, but
        // Flink does not support it.
        // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
        TableParquetOptions {
            global: ParquetOptions {
                writer_version: "1.0".into(),
                compression: Some("snappy".into()),
                ..self.ctx.state().default_table_options().parquet.global
            },
            column_specific_options: HashMap::from([
                (
                    // op column is low cardinality and best encoded as RLE_DICTIONARY
                    self.meta.vocab.operation_type_column.clone(),
                    ParquetColumnOptions {
                        dictionary_enabled: Some(true),
                        ..Default::default()
                    },
                ),
                (
                    self.meta.vocab.system_time_column.clone(),
                    ParquetColumnOptions {
                        // system_time value will be the same for all rows in a batch
                        dictionary_enabled: Some(true),
                        ..Default::default()
                    },
                ),
            ]),
            key_value_metadata: HashMap::new(),
            crypto: datafusion::config::ParquetEncryptionOptions::default(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?path))]
    async fn write_output(
        &self,
        path: PathBuf,
        df: DataFrameExt,
    ) -> Result<Option<OwnedFile>, StageDataError> {
        use datafusion::arrow::array::UInt64Array;

        // FIXME: The  extension is currently necessary for DataFusion to
        // respect the single-file output
        // See: https://github.com/apache/datafusion/issues/13323
        assert!(
            path.extension().is_some(),
            "Ouput file name must have an extension"
        );

        let res = match df
            .write_parquet(
                path.as_os_str().to_str().unwrap(),
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(self.get_write_properties()),
            )
            .await
        {
            Ok(res) => Ok(res),
            Err(datafusion::error::DataFusionError::Execution(msg)) => {
                Err(StageDataError::ExecutionError(ExecutionError::new(msg)))
            }
            Err(err) => Err(err.int_err().into()),
        }?;

        let file = if path.exists() {
            Some(OwnedFile::new(path))
        } else {
            None
        };

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
            let file = file.unwrap();
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
    ) -> Result<(odf::metadata::OffsetInterval, Option<DateTime<Utc>>), InternalError> {
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
                    file_decryption_properties: None,
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
                    min(col(Column::from_name(&self.meta.vocab.offset_column))),
                    max(col(Column::from_name(&self.meta.vocab.offset_column))),
                    // TODO: Add support for more watermark strategies
                    max(col(Column::from_name(&self.meta.vocab.event_time_column))),
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

        let offset_interval = odf::metadata::OffsetInterval {
            start: u64::try_from(offset_min).unwrap(),
            end: u64::try_from(offset_max).unwrap(),
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

    /// Reads output file back to collect sizes of all linked objects, verifying
    /// referential integrity at the same time
    #[tracing::instrument(level = "info", skip_all)]
    async fn compute_linked_objects_summary(
        &self,
        path: &Path,
        schema: &odf::schema::DataSchema,
        data_repo: &dyn odf::storage::ObjectRepository,
    ) -> Result<Option<odf::schema::ext::LinkedObjectsSummary>, StageDataError> {
        use futures::stream::TryStreamExt;

        let mut link_columns = Vec::new();
        for f in &schema.fields {
            Self::collect_link_columns(f, &Vec::new(), &mut link_columns)?;
        }

        if link_columns.is_empty() {
            return Ok(None);
        }

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
                    file_decryption_properties: None,
                },
            )
            .await
            .int_err()?
            .select(link_columns)
            .int_err()?;

        let mut stream = df.execute_stream().await.int_err()?;

        let mut num_objects_naive = 0;
        let mut size_naive = 0;

        while let Some(batch) = stream.try_next().await.int_err()? {
            let mut object_links = Vec::new();

            for c in 0..batch.columns().len() {
                let col = batch.column(c);

                // TODO: Unify with a wrapper type
                if let Some(array) = col.as_string_view_opt() {
                    object_links.extend(array.into_iter().flatten());
                } else if let Some(array) = col.as_string_opt::<i32>() {
                    object_links.extend(array.into_iter().flatten());
                } else if let Some(array) = col.as_string_opt::<i64>() {
                    object_links.extend(array.into_iter().flatten());
                } else {
                    unreachable!(
                        "ObjectLink column is expected to be Utf8 or Utf8View, but got: {:?}",
                        col.data_type()
                    );
                }
            }

            num_objects_naive += u64::try_from(object_links.len()).unwrap();

            // Avoid repeated calls to object store for duplicates, but only within a batch
            let mut known_objects = std::collections::BTreeMap::new();

            for link in &object_links {
                if let Some(size) = known_objects.get(*link) {
                    size_naive += size;
                    continue;
                }

                let hash = odf::Multihash::from_multibase(link).map_err(|e| {
                    StageDataError::DataValidation(InvalidValueError::new(e.to_string()).into())
                })?;

                match data_repo.get_size(&hash).await {
                    Ok(size) => {
                        size_naive += size;
                        known_objects.insert(*link, size);
                    }
                    Err(odf::storage::GetError::NotFound(_)) => {
                        return Err(StageDataError::DataValidation(
                            DanglingReferenceError::new(hash).into(),
                        ));
                    }
                    Err(err) => {
                        return Err(err.int_err().into());
                    }
                }
            }
        }

        Ok(Some(odf::schema::ext::LinkedObjectsSummary {
            num_objects_naive,
            size_naive,
        }))
    }

    fn collect_link_columns(
        field: &odf::schema::DataField,
        path: &Vec<&odf::schema::DataField>,
        cols: &mut Vec<Expr>,
    ) -> Result<(), InternalError> {
        use odf::schema::*;

        match &field.r#type {
            DataType::Option(DataTypeOption { inner }) => {
                // TODO: Avoid cloning
                let f = DataField {
                    r#type: inner.as_ref().clone(),
                    name: field.name.clone(),
                    extra: field.extra.clone(),
                };
                Self::collect_link_columns(&f, path, cols)
            }
            DataType::String(_) => {
                if Self::is_object_link(field)? {
                    if !path.is_empty() {
                        return Err("Nested ObjectLink columns are not yet supported".int_err());
                    }

                    cols.push(col(Column::from_name(&field.name)));
                }
                Ok(())
            }
            DataType::Binary(_)
            | DataType::Bool(_)
            | DataType::Date(_)
            | DataType::Decimal(_)
            | DataType::Duration(_)
            | DataType::Float16(_)
            | DataType::Float32(_)
            | DataType::Float64(_)
            | DataType::Int8(_)
            | DataType::Int16(_)
            | DataType::Int32(_)
            | DataType::Int64(_)
            | DataType::UInt8(_)
            | DataType::UInt16(_)
            | DataType::UInt32(_)
            | DataType::UInt64(_)
            | DataType::Null(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::Map(_) => {
                if Self::is_object_link(field)? {
                    return Err("ObjectLink is only expected on String core type".int_err());
                }
                Ok(())
            }
            DataType::List(_) => {
                if Self::is_object_link(field)? {
                    return Err("Nested ObjectLink columns are not yet supported".int_err());
                }
                Ok(())
            }
            DataType::Struct(t) => {
                if Self::is_object_link(field)? {
                    return Err("ObjectLink is only expected on String core type".int_err());
                }

                let mut subpath = path.clone();
                for sf in &t.fields {
                    subpath.push(sf);
                    Self::collect_link_columns(sf, &subpath, cols)?;
                    subpath.pop();
                }
                Ok(())
            }
        }
    }

    fn is_object_link(field: &odf::schema::DataField) -> Result<bool, InternalError> {
        use odf::schema::ext::*;

        match field.get_extra::<AttrType>().int_err()? {
            Some(AttrType {
                r#type:
                    odf::schema::ext::DataTypeExt::ObjectLink(DataTypeExtObjectLink { link_type }),
            }) => match link_type.as_ref() {
                DataTypeExt::Multihash(_) => Ok(true),
                DataTypeExt::Did(_) | DataTypeExt::ObjectLink(_) | DataTypeExt::Core(_) => {
                    Err(format!("Unsupported link type: {link_type:?}").int_err())
                }
            },
            _ => Ok(false),
        }
    }

    fn merge_strategy_for(
        conf: odf::metadata::MergeStrategy,
        vocab: &odf::metadata::DatasetVocabulary,
    ) -> Arc<dyn MergeStrategy> {
        use crate::merge_strategies::*;

        match conf {
            odf::metadata::MergeStrategy::Append(_cfg) => {
                Arc::new(MergeStrategyAppend::new(vocab.clone()))
            }
            odf::metadata::MergeStrategy::ChangelogStream(cfg) => {
                Arc::new(MergeStrategyChangelogStream::new(vocab.clone(), cfg))
            }
            odf::metadata::MergeStrategy::Ledger(cfg) => {
                Arc::new(MergeStrategyLedger::new(vocab.clone(), cfg))
            }
            odf::metadata::MergeStrategy::Snapshot(cfg) => {
                Arc::new(MergeStrategySnapshot::new(vocab.clone(), cfg))
            }
            odf::metadata::MergeStrategy::UpsertStream(cfg) => {
                Arc::new(MergeStrategyUpsertStream::new(vocab.clone(), cfg))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DataWriter for DataWriterDataFusion {
    #[tracing::instrument(level = "info", skip_all)]
    async fn write(
        &mut self,
        new_data: Option<DataFrameExt>,
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

        let add_data = odf::dataset::AddDataParams {
            prev_checkpoint: self.meta.prev_checkpoint.clone(),
            prev_offset: self.meta.prev_offset,
            new_offset_interval: None,
            new_linked_objects: None,
            new_watermark: Some(new_watermark),
            new_source_state: opts.new_source_state,
        };

        let staged = StageDataResult {
            system_time: opts.system_time,
            add_data: Some(add_data),
            new_schema: None,
            data_file: None,
        };

        let commit = self.commit(staged).await?;
        Ok(commit)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn stage(
        &self,
        new_data: Option<DataFrameExt>,
        opts: WriteDataOpts,
    ) -> Result<StageDataResult, StageDataError> {
        let (add_data, new_schema, data_file) = if let Some(new_data) = new_data {
            let dataset_schema_arrow = self
                .meta
                .schema
                .as_ref()
                .map(|s| s.to_arrow(&odf::metadata::ToArrowSettings::default()))
                .transpose()
                .int_err()?
                .map(Arc::new);

            self.validate_input(&new_data)?;

            // Normalize timestamps
            let df = self.normalize_raw_result(new_data)?;

            // Merge step
            // TODO: PERF: We could likely benefit from checkpointing here
            let prev = self
                .get_all_previous_data(&self.meta.data_slices, dataset_schema_arrow.as_ref())
                .await?;

            // Populate event time with nulls if missing, using matching type to prev data
            let df = self.ensure_event_time_column(df, prev.as_ref().map(DataFrameExt::schema))?;

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

            // Coerce schema if needed
            let df = self.coerce_schema(df)?;

            // Validate schema matches the declared one
            let new_slice_schema_arrow = SchemaRef::new(df.schema().into());
            tracing::info!(?new_slice_schema_arrow, "Final output schema");

            if let Some(dataset_schema_arrow) = &dataset_schema_arrow {
                Self::validate_schema_compatible(dataset_schema_arrow, &new_slice_schema_arrow)?;
            }

            // NOTE: We strip encoding to store only logical representation of schema in the
            // event
            let new_schema = odf::schema::DataSchema::new_from_arrow(&new_slice_schema_arrow)
                .int_err()?
                .strip_encoding();

            // Write output
            let data_file = self.write_output(opts.data_staging_path, df).await?;

            // Prepare commit info
            let prev_offset = self.meta.prev_offset;
            let prev_checkpoint = self.meta.prev_checkpoint.clone();
            let new_source_state = opts.new_source_state;
            let prev_watermark = self.meta.prev_watermark;

            if let Some(data_file) = data_file {
                let (new_offset_interval, new_watermark_from_data) = self
                    .compute_offset_and_watermark(data_file.as_path(), prev_watermark)
                    .await?;

                let new_linked_objects = self
                    .compute_linked_objects_summary(
                        data_file.as_path(),
                        self.meta.schema.as_ref().unwrap_or(&new_schema),
                        self.target.as_data_repo(),
                    )
                    .await?;

                (
                    odf::dataset::AddDataParams {
                        prev_checkpoint,
                        prev_offset,
                        new_offset_interval: Some(new_offset_interval),
                        new_linked_objects,
                        new_watermark: opts.new_watermark.or(new_watermark_from_data),
                        new_source_state,
                    },
                    Some(new_schema),
                    Some(data_file),
                )
            } else {
                // Empty result - carry watermark and propagate source state
                (
                    odf::dataset::AddDataParams {
                        prev_checkpoint,
                        prev_offset,
                        new_offset_interval: None,
                        new_linked_objects: None,
                        new_watermark: opts.new_watermark.or(prev_watermark),
                        new_source_state,
                    },
                    Some(new_schema),
                    None,
                )
            }
        } else {
            // TODO: Should watermark be advanced by the source event time?
            let add_data = odf::dataset::AddDataParams {
                prev_checkpoint: self.meta.prev_checkpoint.clone(),
                prev_offset: self.meta.prev_offset,
                new_offset_interval: None,
                new_linked_objects: None,
                new_watermark: self.meta.prev_watermark,
                new_source_state: opts.new_source_state,
            };

            (add_data, None, None)
        };

        // Do we need to commit `SetDataSchema` event?
        let new_schema = if self.meta.schema.is_none() {
            new_schema
        } else {
            None
        };

        // Do we have anything to commit in `AddData` event?
        let add_data = if add_data.new_offset_interval.is_some()
            || add_data.new_watermark != self.meta.prev_watermark
            || add_data.new_source_state != self.meta.prev_source_state
        {
            Some(add_data)
        } else {
            None
        };

        if new_schema.is_some() || add_data.is_some() {
            Ok(StageDataResult {
                system_time: opts.system_time,
                add_data,
                new_schema,
                data_file,
            })
        } else {
            Err(EmptyCommitError {}.into())
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn commit(
        &mut self,
        staged: StageDataResult,
    ) -> Result<WriteDataResult, odf::dataset::CommitError> {
        assert!(staged.new_schema.is_some() || staged.add_data.is_some());

        let old_head = self.meta.head.clone();

        // Commit `SetDataSchema` event
        if let Some(new_schema) = staged.new_schema {
            let set_data_schema = odf::metadata::SetDataSchema::new(new_schema.clone());

            let commit_schema_result = self
                .target
                .commit_event(
                    set_data_schema.clone().into(),
                    odf::dataset::CommitOpts {
                        block_ref: &self.meta.block_ref,
                        system_time: Some(staged.system_time),
                        prev_block_hash: Some(Some(&self.meta.head)),
                        check_object_refs: false,
                        update_block_ref: false,
                    },
                )
                .await?;

            // Update state
            self.meta.head = commit_schema_result.new_head;
            self.meta.schema = Some(new_schema);
        }

        // Commit `AddData` event
        let add_data_block = if let Some(add_data) = staged.add_data {
            let commit_data_result = self
                .target
                .commit_add_data(
                    add_data,
                    staged.data_file,
                    None,
                    odf::dataset::CommitOpts {
                        block_ref: &self.meta.block_ref,
                        system_time: Some(staged.system_time),
                        prev_block_hash: Some(Some(&self.meta.head)),
                        check_object_refs: false,
                        update_block_ref: false,
                    },
                )
                .await?;

            // Update state for the next append
            use odf::metadata::AsTypedBlock;
            let new_block = self
                .target
                .as_metadata_chain()
                .get_block(&commit_data_result.new_head)
                .await
                .int_err()?
                .into_typed::<odf::metadata::AddData>()
                .unwrap();

            self.meta.head = commit_data_result.new_head;

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
            self.meta
                .prev_source_state
                .clone_from(&new_block.event.new_source_state);

            Some(new_block)
        } else {
            None
        };

        Ok(WriteDataResult {
            old_head,
            new_head: self.meta.head.clone(),
            add_data_block,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
