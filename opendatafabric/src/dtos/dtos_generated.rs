// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
///////////////////////////////////////////////////////////////////////////////

use crate::formats::Multihash;
use crate::identity::{DatasetID, DatasetName};
use chrono::{DateTime, Utc};
use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

/// Indicates that data has been ingested into a root dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddData {
    /// Hash of the checkpoint file used to restore ingestion state, if any.
    pub input_checkpoint: Option<Multihash>,
    /// Describes output data written during this transaction.
    pub output_data: DataSlice,
    /// Describes checkpoint written during this transaction, if any.
    pub output_checkpoint: Option<Checkpoint>,
    /// Last watermark of the output data stream.
    pub output_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////

/// Embedded attachment item.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentEmbedded {
    /// Path to an attachment if it was materialized into a file.
    pub path: String,
    /// Content of the attachment.
    pub content: String,
}

////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

/// For attachments that are specified inline and are embedded in the metadata.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentsEmbedded {
    pub items: Vec<AttachmentEmbedded>,
}

////////////////////////////////////////////////////////////////////////////////
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a range of metadata blocks as an arithmetic interval of block hashes
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockInterval {
    /// Start of the closed interval [start; end]
    pub start: Multihash,
    /// End of the closed interval [start; end]
    pub end: Multihash,
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a checkpoint produced by an engine
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Checkpoint {
    /// Hash sum of the checkpoint file
    pub physical_hash: Multihash,
    /// Size of checkpoint file in bytes
    pub size: i64,
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of data added to a dataset or produced via transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataSlice {
    /// Logical hash sum of the data in this slice
    pub logical_hash: Multihash,
    /// Hash sum of the data part file
    pub physical_hash: Multihash,
    /// Data slice produced by the transaction
    pub interval: OffsetInterval,
    /// Size of data file in bytes
    pub size: i64,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatasetKind {
    Root,
    Derivative,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

/// Represents a snapshot of the dataset definition in a single point in time.
/// This type is typically used for defining new datasets and changing the existing ones.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSnapshot {
    /// Alias of the dataset.
    pub name: DatasetName,
    /// Type of the dataset.
    pub kind: DatasetKind,
    pub metadata: Vec<MetadataEvent>,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

/// Lets you manipulate names of the system columns to avoid conflicts.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetVocabulary {
    /// Name of the system time column.
    pub system_time_column: Option<String>,
    /// Name of the event time column.
    pub event_time_column: Option<String>,
    /// Name of the offset column.
    pub offset_column: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////

/// Defines an environment variable passed into some job.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EnvVar {
    /// Name of the variable.
    pub name: String,
    /// Value of the variable.
    pub value: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum EventTimeSource {
    FromMetadata,
    FromPath(EventTimeSourceFromPath),
}

/// Extracts event time from the path component of the source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromPath {
    /// Regular expression where first group contains the timestamp string.
    pub pattern: String,
    /// Format of the expected timestamp in java.text.SimpleDateFormat form.
    pub timestamp_format: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

/// Indicates that derivative transformation has been performed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQuery {
    /// Defines inputs used in this transaction. Slices corresponding to every input must be present.
    pub input_slices: Vec<InputSlice>,
    /// Hash of the checkpoint file used to restore transformation state, if any.
    pub input_checkpoint: Option<Multihash>,
    /// Describes output data written during this transaction, if any.
    pub output_data: Option<DataSlice>,
    /// Describes checkpoint written during this transaction, if any.
    pub output_checkpoint: Option<Checkpoint>,
    /// Last watermark of the output data stream.
    pub output_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

/// Sent as part of the execute query requst to describe the input
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryInput {
    /// Unique identifier of the dataset.
    pub dataset_id: DatasetID,
    /// An alias of this input to be used in queries.
    pub dataset_name: DatasetName,
    pub vocab: DatasetVocabulary,
    /// Data that went into this transaction.
    pub data_interval: Option<OffsetInterval>,
    /// TODO: This will be removed when coordinator will be slicing data for the engine
    pub data_paths: Vec<PathBuf>,
    /// TODO: replace with actual DDL or Parquet schema
    pub schema_file: PathBuf,
    pub explicit_watermarks: Vec<Watermark>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform the next step of data transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryRequest {
    /// Unique identifier of the output dataset
    pub dataset_id: DatasetID,
    /// Alias of the output dataset
    pub dataset_name: DatasetName,
    /// System time to use for new records
    pub system_time: DateTime<Utc>,
    /// Starting offset to use for new records
    pub offset: i64,
    pub vocab: DatasetVocabulary,
    /// Transformation that will be applied to produce new data
    pub transform: Transform,
    /// Defines the input data
    pub inputs: Vec<ExecuteQueryInput>,
    /// TODO: This is temporary
    pub prev_checkpoint_path: Option<PathBuf>,
    /// TODO: This is temporary
    pub new_checkpoint_path: PathBuf,
    /// TODO: This is temporary
    pub out_data_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryresponse-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ExecuteQueryResponse {
    Progress,
    Success(ExecuteQueryResponseSuccess),
    InvalidQuery(ExecuteQueryResponseInvalidQuery),
    InternalError(ExecuteQueryResponseInternalError),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryResponseSuccess {
    /// Data slice produced by the transaction (if any)
    pub data_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction (uf any).
    pub output_watermark: Option<DateTime<Utc>>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
}

/// Pulls data from one of the supported sources by its URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepUrl {
    /// URL of the data source
    pub url: String,
    /// Describes how event time is extracted from the source metadata.
    pub event_time: Option<EventTimeSource>,
    /// Describes the caching settings used for this source.
    pub cache: Option<SourceCaching>,
}

/// Uses glob operator to match files on the local file system.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepFilesGlob {
    /// Path with a glob pattern.
    pub path: String,
    /// Describes how event time is extracted from the source metadata.
    pub event_time: Option<EventTimeSource>,
    /// Describes the caching settings used for this source.
    pub cache: Option<SourceCaching>,
    /// Specifies how input files should be ordered before ingestion.
    /// Order is important as every file will be processed individually
    /// and will advance the dataset's watermark.
    pub order: Option<SourceOrdering>,
}

/// Runs the specified OCI container to fetch data from an arbitrary source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepContainer {
    /// Image name and and an optional tag.
    pub image: String,
    /// Specifies the entrypoint. Not executed within a shell. The default OCI image's ENTRYPOINT is used if this is not provided.
    pub command: Option<Vec<String>>,
    /// Arguments to the entrypoint. The OCI image's CMD is used if this is not provided.
    pub args: Option<Vec<String>>,
    /// Environment variables to propagate into or set in the container.
    pub env: Option<Vec<EnvVar>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

////////////////////////////////////////////////////////////////////////////////
// InputSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#inputslice-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of input data used during a transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InputSlice {
    /// Input dataset identifier
    pub dataset_id: DatasetID,
    /// Blocks that went into this transaction
    pub block_interval: Option<BlockInterval>,
    /// Data that went into this transaction
    pub data_interval: Option<OffsetInterval>,
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MergeStrategy {
    Append,
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
}

/// Ledger merge strategy.
///
/// This strategy should be used for data sources containing append-only event
/// streams. New data dumps can have new rows added, but once data already
/// made it into one dump it never changes or disappears.
///
/// A system time column will be added to the data to indicate the time
/// when the record was observed first by the system.
///
/// It relies on a user-specified primary key columns to identify which records
/// were already seen and not duplicate them.
///
/// It will always preserve all columns from existing and new snapshots, so
/// the set of columns can only grow.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyLedger {
    /// Names of the columns that uniquely identify the record throughout its lifetime
    pub primary_key: Vec<String>,
}

/// Snapshot merge strategy.
///
/// This strategy can be used for data dumps that are taken periodical
/// and contain only the latest state of the observed entity or system.
/// Over time such dumps can have new rows added, and old rows either removed
/// or modified.
///
/// This strategy transforms snapshot data into an append-only event stream
/// where data already added is immutable. It does so by treating rows in
/// snapshots as "observation" events and adding an "observed" column
/// that will contain:
/// - "I" - when a row appears for the first time
/// - "D" - when row disappears
/// - "U" - whenever any row data has changed
///
/// It relies on a user-specified primary key columns to correlate the rows
/// between the two snapshots.
///
/// The time when a snapshot was taken (event time) is usually captured in some
/// form of metadata (e.g. in the name of the snapshot file, or in the caching
/// headers). In order to populate the event time we rely on the `FetchStep`
/// to extract the event time from metadata. User then should specify the name
/// of the event time column that will be populated from this value.
///
/// If the data contains a column that is guaranteed to change whenever
/// any of the data columns changes (for example this can be a last
/// modification timestamp, an incremental version, or a data hash), then
/// it can be specified as modification indicator to speed up the detection of
/// modified rows.
///
/// Schema Changes:
///
/// This strategy will always preserve all columns from the existing and new snapshots, so the set of columns can only grow.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategySnapshot {
    /// Names of the columns that uniquely identify the record throughout its lifetime.
    pub primary_key: Vec<String>,
    /// Names of the columns to compared to determine if a row has changed between two snapshots.
    pub compare_columns: Option<Vec<String>>,
    /// Name of the observation type column that will be added to the data.
    pub observation_column: Option<String>,
    /// Name of the observation type when the data with certain primary key is seen for the first time.
    pub obsv_added: Option<String>,
    /// Name of the observation type when the data with certain primary key has changed compared to the last time it was seen.
    pub obsv_changed: Option<String>,
    /// Name of the observation type when the data with certain primary key has been seen before but now is missing from the snapshot.
    pub obsv_removed: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

/// An individual block in the metadata chain that captures the history of modifications of a dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MetadataBlock {
    /// System time when this block was written.
    pub system_time: DateTime<Utc>,
    /// Hash sum of the preceding block.
    pub prev_block_hash: Option<Multihash>,
    /// Event data.
    pub event: MetadataEvent,
}

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MetadataEvent {
    AddData(AddData),
    ExecuteQuery(ExecuteQuery),
    Seed(Seed),
    SetPollingSource(SetPollingSource),
    SetTransform(SetTransform),
    SetVocab(SetVocab),
    SetWatermark(SetWatermark),
    SetAttachments(SetAttachments),
    SetInfo(SetInfo),
    SetLicense(SetLicense),
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a range of data as an arithmetic interval of offsets
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OffsetInterval {
    /// Start of the closed interval [start; end]
    pub start: i64,
    /// End of the closed interval [start; end]
    pub end: i64,
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

/// Pulls data from one of the supported sources by its URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepDecompress {
    /// Name of a compression algorithm used on data.
    pub format: CompressionFormat,
    /// Path to a data file within a multi-file archive. Can contain glob patterns.
    pub sub_path: Option<String>,
}

/// Executes external command to process the data using piped input/output.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepPipe {
    /// Command to execute and its arguments.
    pub command: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionFormat {
    Gzip,
    Zip,
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReadStep {
    Csv(ReadStepCsv),
    JsonLines(ReadStepJsonLines),
    GeoJson(ReadStepGeoJson),
    EsriShapefile(ReadStepEsriShapefile),
}

/// Reader for comma-separated files.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepCsv {
    /// A DDL-formatted schema. Schema can be used to coerce values into more appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets a single character as a separator for each field and value.
    pub separator: Option<String>,
    /// Decodes the CSV files by the given encoding type.
    pub encoding: Option<String>,
    /// Sets a single character used for escaping quoted values where the separator can be part of the value. Set an empty string to turn off quotations.
    pub quote: Option<String>,
    /// Sets a single character used for escaping quotes inside an already quoted value.
    pub escape: Option<String>,
    /// Sets a single character used for skipping lines beginning with this character.
    pub comment: Option<String>,
    /// Use the first line as names of columns.
    pub header: Option<bool>,
    /// If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true.
    pub enforce_schema: Option<bool>,
    /// Infers the input schema automatically from data. It requires one extra pass over the data.
    pub infer_schema: Option<bool>,
    /// A flag indicating whether or not leading whitespaces from values being read should be skipped.
    pub ignore_leading_white_space: Option<bool>,
    /// A flag indicating whether or not trailing whitespaces from values being read should be skipped.
    pub ignore_trailing_white_space: Option<bool>,
    /// Sets the string representation of a null value.
    pub null_value: Option<String>,
    /// Sets the string representation of an empty value.
    pub empty_value: Option<String>,
    /// Sets the string representation of a non-number value.
    pub nan_value: Option<String>,
    /// Sets the string representation of a positive infinity value.
    pub positive_inf: Option<String>,
    /// Sets the string representation of a negative infinity value.
    pub negative_inf: Option<String>,
    /// Sets the string that indicates a date format.
    pub date_format: Option<String>,
    /// Sets the string that indicates a timestamp format.
    pub timestamp_format: Option<String>,
    /// Parse one record, which may span multiple lines.
    pub multi_line: Option<bool>,
}

/// Reader for files containing concatenation of multiple JSON records with the same schema.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepJsonLines {
    /// A DDL-formatted schema. Schema can be used to coerce values into more appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets the string that indicates a date format.
    pub date_format: Option<String>,
    /// Allows to forcibly set one of standard basic or extended encoding.
    pub encoding: Option<String>,
    /// Parse one record, which may span multiple lines, per file.
    pub multi_line: Option<bool>,
    /// Infers all primitive values as a string type.
    pub primitives_as_string: Option<bool>,
    /// Sets the string that indicates a timestamp format.
    pub timestamp_format: Option<String>,
}

/// Reader for GeoJSON files.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more appropriate data types.
    pub schema: Option<Vec<String>>,
}

/// Reader for ESRI Shapefile format.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepEsriShapefile {
    /// A DDL-formatted schema. Schema can be used to coerce values into more appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Path to a data file within a multi-file archive. Can contain glob patterns.
    pub sub_path: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

/// Establishes the identity of the dataset. Always the first metadata event in the chain.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seed {
    /// Unique identity of the dataset.
    pub dataset_id: DatasetID,
    /// Type of the dataset.
    pub dataset_kind: DatasetKind,
}

////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////

/// Associates a set of files with this dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetAttachments {
    /// One of the supported attachment sources.
    pub attachments: Attachments,
}

////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////

/// Provides basic human-readable information about a dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetInfo {
    /// Brief single-sentence summary of a dataset.
    pub description: Option<String>,
    /// Keywords, search terms, or tags used to describe the dataset.
    pub keywords: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////

/// Defines a license that applies to this dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetLicense {
    /// Abbriviated name of the license.
    pub short_name: String,
    /// Full name of the license.
    pub name: String,
    /// License identifier from the SPDX License List.
    pub spdx_id: Option<String>,
    pub website_url: String,
}

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

/// Contains information on how extenrally-hosted data can be ingested into the root dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetPollingSource {
    /// Determines where data is sourced from.
    pub fetch: FetchStep,
    /// Defines how raw data is prepared before reading.
    pub prepare: Option<Vec<PrepStep>>,
    /// Defines how data is read into structured format.
    pub read: ReadStep,
    /// Pre-processing query that shapes the data.
    pub preprocess: Option<Transform>,
    /// Determines how newly-ingested data should be merged with existing history.
    pub merge: MergeStrategy,
}

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

/// Defines a transformation that produces data in a derivative dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetTransform {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

/// Specifies the mapping of system columns onto dataset schema.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetVocab {
    /// Name of the system time column.
    pub system_time_column: Option<String>,
    /// Name of the event time column.
    pub event_time_column: Option<String>,
    /// Name of the offset column.
    pub offset_column: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

/// Indicates the advancement of the dataset's watermark.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetWatermark {
    /// Last watermark of the output data stream.
    pub output_watermark: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SourceCaching {
    Forever,
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////

/// Defines a query in a multi-step SQL transformation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SqlQueryStep {
    /// Name of the temporary view that will be created from result of the query.
    pub alias: Option<String>,
    /// SQL query the result of which will be exposed under the alias.
    pub query: String,
}

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

/// Temporary Flink-specific extension for creating temporal tables from streams.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TemporalTable {
    /// Name of the dataset to be converted into a temporal table.
    pub name: String,
    /// Column names used as the primary key for creating a table.
    pub primary_key: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Transform {
    Sql(TransformSql),
}

/// Transform using one of the SQL dialects.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformSql {
    /// Identifier of the engine used for this transformation.
    pub engine: String,
    /// Version of the engine to use.
    pub version: Option<String>,
    /// SQL query the result of which will be used as an output.
    pub query: Option<String>,
    /// Use this instead of query field for specifying multi-step SQL transformations. Each step acts as a shorthand for `CREATE TEMPORARY VIEW <alias> AS (<query>)`.
    pub queries: Option<Vec<SqlQueryStep>>,
    /// Temporary Flink-specific extension for creating temporal tables from streams.
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

/// Describes a derivative transformation input
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformInput {
    /// Unique dataset identifier. This field is required in metadata blocks and can be empty only in a DatasetSnapshot.
    pub id: Option<DatasetID>,
    /// An alias of this input to be used in queries.
    pub name: DatasetName,
}

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

/// Represents a watermark in the event stream.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Watermark {
    pub system_time: DateTime<Utc>,
    pub event_time: DateTime<Utc>,
}
