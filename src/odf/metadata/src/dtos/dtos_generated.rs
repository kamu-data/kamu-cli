// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use enum_variants::*;

use crate::formats::Multihash;
use crate::identity::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that data has been ingested into a root dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddData {
    /// Hash of the checkpoint file used to restore ingestion state, if any.
    pub prev_checkpoint: Option<Multihash>,
    /// Last offset of the previous data slice, if any. Must be equal to the
    /// last non-empty `newData.offsetInterval.end`.
    pub prev_offset: Option<u64>,
    /// Describes output data written during this transaction, if any.
    pub new_data: Option<DataSlice>,
    /// Describes checkpoint written during this transaction, if any. If an
    /// engine operation resulted in no updates to the checkpoint, but
    /// checkpoint is still relevant for subsequent runs - a hash of the
    /// previous checkpoint should be specified.
    pub new_checkpoint: Option<Checkpoint>,
    /// Last watermark of the output data stream, if any. Initial blocks may not
    /// have watermarks, but once watermark is set - all subsequent blocks
    /// should either carry the same watermark or specify a new (greater) one.
    /// Thus, watermarks are monotonically non-decreasing.
    pub new_watermark: Option<DateTime<Utc>>,
    /// The state of the source the data was added from to allow fast resuming.
    /// If the state did not change but is still relevant for subsequent runs it
    /// should be carried, i.e. only the last state per source is considered
    /// when resuming.
    pub new_source_state: Option<SourceState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes how to ingest data into a root dataset from a certain logical
/// source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddPushSource {
    /// Identifies the source within this dataset.
    pub source_name: String,
    /// Defines how data is read into structured format.
    pub read: ReadStep,
    /// Pre-processing query that shapes the data.
    pub preprocess: Option<Transform>,
    /// Determines how newly-ingested data should be merged with existing
    /// history.
    pub merge: MergeStrategy,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Embedded attachment item.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentEmbedded {
    /// Path to an attachment if it was materialized into a file.
    pub path: String,
    /// Content of the attachment.
    pub content: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

impl_enum_with_variants!(Attachments);

/// For attachments that are specified inline and are embedded in the metadata.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentsEmbedded {
    /// List of embedded items.
    pub items: Vec<AttachmentEmbedded>,
}

impl_enum_variant!(Attachments::Embedded(AttachmentsEmbedded));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a checkpoint produced by an engine
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Checkpoint {
    /// Hash sum of the checkpoint file.
    pub physical_hash: Multihash,
    /// Size of checkpoint file in bytes.
    pub size: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompressionFormat
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionFormat {
    Gzip,
    Zip,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of data added to a dataset or produced via transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataSlice {
    /// Logical hash sum of the data in this slice.
    pub logical_hash: Multihash,
    /// Hash sum of the data part file.
    pub physical_hash: Multihash,
    /// Data slice produced by the transaction.
    pub offset_interval: OffsetInterval,
    /// Size of data file in bytes.
    pub size: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatasetKind {
    Root,
    Derivative,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a projection of the dataset metadata at a single point in time.
/// This type is typically used for defining new datasets and changing the
/// existing ones.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSnapshot {
    /// Alias of the dataset.
    pub name: DatasetAlias,
    /// Type of the dataset.
    pub kind: DatasetKind,
    /// An array of metadata events that will be used to populate the chain.
    /// Here you can define polling and push sources, set licenses, add
    /// attachments etc.
    pub metadata: Vec<MetadataEvent>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the mapping of system columns onto dataset schema.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetVocabulary {
    /// Name of the offset column.
    pub offset_column: String,
    /// Name of the operation type column.
    pub operation_type_column: String,
    /// Name of the system time column.
    pub system_time_column: String,
    /// Name of the event time column.
    pub event_time_column: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined polling source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DisablePollingSource {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DisablePushSource {
    /// Identifies the source to be disabled.
    pub source_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines an environment variable passed into some job.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EnvVar {
    /// Name of the variable.
    pub name: String,
    /// Value of the variable.
    pub value: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum EventTimeSource {
    FromMetadata(EventTimeSourceFromMetadata),
    FromPath(EventTimeSourceFromPath),
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl_enum_with_variants!(EventTimeSource);

/// Extracts event time from the source's metadata.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromMetadata {}

impl_enum_variant!(EventTimeSource::FromMetadata(EventTimeSourceFromMetadata));

/// Extracts event time from the path component of the source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromPath {
    /// Regular expression where first group contains the timestamp string.
    pub pattern: String,
    /// Format of the expected timestamp in java.text.SimpleDateFormat form.
    pub timestamp_format: Option<String>,
}

impl_enum_variant!(EventTimeSource::FromPath(EventTimeSourceFromPath));

/// Assigns event time from the system time source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromSystemTime {}

impl_enum_variant!(EventTimeSource::FromSystemTime(
    EventTimeSourceFromSystemTime
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that derivative transformation has been performed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteTransform {
    /// Defines inputs used in this transaction. Slices corresponding to every
    /// input dataset must be present.
    pub query_inputs: Vec<ExecuteTransformInput>,
    /// Hash of the checkpoint file used to restore transformation state, if
    /// any.
    pub prev_checkpoint: Option<Multihash>,
    /// Last offset of the previous data slice, if any. Must be equal to the
    /// last non-empty `newData.offsetInterval.end`.
    pub prev_offset: Option<u64>,
    /// Describes output data written during this transaction, if any.
    pub new_data: Option<DataSlice>,
    /// Describes checkpoint written during this transaction, if any. If an
    /// engine operation resulted in no updates to the checkpoint, but
    /// checkpoint is still relevant for subsequent runs - a hash of the
    /// previous checkpoint should be specified.
    pub new_checkpoint: Option<Checkpoint>,
    /// Last watermark of the output data stream, if any. Initial blocks may not
    /// have watermarks, but once watermark is set - all subsequent blocks
    /// should either carry the same watermark or specify a new (greater) one.
    /// Thus, watermarks are monotonically non-decreasing.
    pub new_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of the input dataset used during a transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteTransformInput {
    /// Input dataset identifier.
    pub dataset_id: DatasetID,
    /// Last block of the input dataset that was previously incorporated into
    /// the derivative transformation, if any. Must be equal to the last
    /// non-empty `newBlockHash`. Together with `newBlockHash` defines a
    /// half-open `(prevBlockHash, newBlockHash]` interval of blocks that will
    /// be considered in this transaction.
    pub prev_block_hash: Option<Multihash>,
    /// Hash of the last block that will be incorporated into the derivative
    /// transformation. When present, defines a half-open `(prevBlockHash,
    /// newBlockHash]` interval of blocks that will be considered in this
    /// transaction.
    pub new_block_hash: Option<Multihash>,
    /// Last data record offset in the input dataset that was previously
    /// incorporated into the derivative transformation, if any. Must be equal
    /// to the last non-empty `newOffset`. Together with `newOffset` defines a
    /// half-open `(prevOffset, newOffset]` interval of data records that will
    /// be considered in this transaction.
    pub prev_offset: Option<u64>,
    /// Offset of the last data record that will be incorporated into the
    /// derivative transformation, if any. When present, defines a half-open
    /// `(prevOffset, newOffset]` interval of data records that will be
    /// considered in this transaction.
    pub new_offset: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
    Mqtt(FetchStepMqtt),
    EthereumLogs(FetchStepEthereumLogs),
}

impl_enum_with_variants!(FetchStep);

/// Pulls data from one of the supported sources by its URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepUrl {
    /// URL of the data source
    pub url: String,
    /// Describes how event time is extracted from the source metadata.
    pub event_time: Option<EventTimeSource>,
    /// Describes the caching settings used for this source.
    pub cache: Option<SourceCaching>,
    /// Headers to pass during the request (e.g. HTTP Authorization)
    pub headers: Option<Vec<RequestHeader>>,
}

impl_enum_variant!(FetchStep::Url(FetchStepUrl));

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

impl_enum_variant!(FetchStep::FilesGlob(FetchStepFilesGlob));

/// Runs the specified OCI container to fetch data from an arbitrary source.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepContainer {
    /// Image name and and an optional tag.
    pub image: String,
    /// Specifies the entrypoint. Not executed within a shell. The default OCI
    /// image's ENTRYPOINT is used if this is not provided.
    pub command: Option<Vec<String>>,
    /// Arguments to the entrypoint. The OCI image's CMD is used if this is not
    /// provided.
    pub args: Option<Vec<String>>,
    /// Environment variables to propagate into or set in the container.
    pub env: Option<Vec<EnvVar>>,
}

impl_enum_variant!(FetchStep::Container(FetchStepContainer));

/// Connects to an MQTT broker to fetch events from the specified topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepMqtt {
    /// Hostname of the MQTT broker.
    pub host: String,
    /// Port of the MQTT broker.
    pub port: i32,
    /// Username to use for auth with the broker.
    pub username: Option<String>,
    /// Password to use for auth with the broker (can be templated).
    pub password: Option<String>,
    /// List of topic subscription parameters.
    pub topics: Vec<MqttTopicSubscription>,
}

impl_enum_variant!(FetchStep::Mqtt(FetchStepMqtt));

/// Connects to an Ethereum node to stream transaction logs.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepEthereumLogs {
    /// Identifier of the chain to scan logs from. This parameter may be used
    /// for RPC endpoint lookup as well as asserting that provided `nodeUrl`
    /// corresponds to the expected chain.
    pub chain_id: Option<u64>,
    /// Url of the node.
    pub node_url: Option<String>,
    /// An SQL WHERE clause that can be used to pre-filter the logs before
    /// fetching them from the ETH node.
    pub filter: Option<String>,
    /// Solidity log event signature to use for decoding. Using this field adds
    /// `event` to the output containing decoded log as JSON.
    pub signature: Option<String>,
}

impl_enum_variant!(FetchStep::EthereumLogs(FetchStepEthereumLogs));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
}

impl_enum_with_variants!(MergeStrategy);

/// Append merge strategy.
///
/// Under this strategy new data will be appended to the dataset in its
/// entirety, without any deduplication.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyAppend {}

impl_enum_variant!(MergeStrategy::Append(MergeStrategyAppend));

/// Ledger merge strategy.
///
/// This strategy should be used for data sources containing ledgers of events.
/// Currently this strategy will only perform deduplication of events using
/// user-specified primary key columns. This means that the source data can
/// contain partially overlapping set of records and only those records that
/// were not previously seen will be appended.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyLedger {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl_enum_variant!(MergeStrategy::Ledger(MergeStrategyLedger));

/// Snapshot merge strategy.
///
/// This strategy can be used for data state snapshots that are taken
/// periodically and contain only the latest state of the observed entity or
/// system. Over time such snapshots can have new rows added, and old rows
/// either removed or modified.
///
/// This strategy transforms snapshot data into an append-only event stream
/// where data already added is immutable. It does so by performing Change Data
/// Capture - essentially diffing the current state of data against the
/// reconstructed previous state and recording differences as retractions or
/// corrections. The Operation Type "op" column will contain:
/// - append (`+A`) when a row appears for the first time
/// - retraction (`-D`) when row disappears
/// - correction (`-C`, `+C`) when row data has changed, with `-C` event
///   carrying the old value of the row and `+C` carrying the new value.
///
/// To correctly associate rows between old and new snapshots this strategy
/// relies on user-specified primary key columns.
///
/// To identify whether a row has changed this strategy will compare all other
/// columns one by one. If the data contains a column that is guaranteed to
/// change whenever any of the data columns changes (for example a last
/// modification timestamp, an incremental version, or a data hash), then it can
/// be specified in `compareColumns` property to speed up the detection of
/// modified rows.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategySnapshot {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime.
    pub primary_key: Vec<String>,
    /// Names of the columns to compared to determine if a row has changed
    /// between two snapshots.
    pub compare_columns: Option<Vec<String>>,
}

impl_enum_variant!(MergeStrategy::Snapshot(MergeStrategySnapshot));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An individual block in the metadata chain that captures the history of
/// modifications of a dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MetadataBlock {
    /// System time when this block was written.
    pub system_time: DateTime<Utc>,
    /// Hash sum of the preceding block.
    pub prev_block_hash: Option<Multihash>,
    /// Block sequence number, starting from zero at the seed block.
    pub sequence_number: u64,
    /// Event data.
    pub event: MetadataEvent,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MetadataEvent {
    AddData(AddData),
    ExecuteTransform(ExecuteTransform),
    Seed(Seed),
    SetPollingSource(SetPollingSource),
    SetTransform(SetTransform),
    SetVocab(SetVocab),
    SetAttachments(SetAttachments),
    SetInfo(SetInfo),
    SetLicense(SetLicense),
    SetDataSchema(SetDataSchema),
    AddPushSource(AddPushSource),
    DisablePushSource(DisablePushSource),
    DisablePollingSource(DisablePollingSource),
}

impl_enum_with_variants!(MetadataEvent);

impl_enum_variant!(MetadataEvent::AddData(AddData));

impl_enum_variant!(MetadataEvent::ExecuteTransform(ExecuteTransform));

impl_enum_variant!(MetadataEvent::Seed(Seed));

impl_enum_variant!(MetadataEvent::SetPollingSource(SetPollingSource));

impl_enum_variant!(MetadataEvent::SetTransform(SetTransform));

impl_enum_variant!(MetadataEvent::SetVocab(SetVocab));

impl_enum_variant!(MetadataEvent::SetAttachments(SetAttachments));

impl_enum_variant!(MetadataEvent::SetInfo(SetInfo));

impl_enum_variant!(MetadataEvent::SetLicense(SetLicense));

impl_enum_variant!(MetadataEvent::SetDataSchema(SetDataSchema));

impl_enum_variant!(MetadataEvent::AddPushSource(AddPushSource));

impl_enum_variant!(MetadataEvent::DisablePushSource(DisablePushSource));

impl_enum_variant!(MetadataEvent::DisablePollingSource(DisablePollingSource));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT topic subscription parameters.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MqttTopicSubscription {
    /// Name of the topic (may include patterns).
    pub path: String,
    /// Quality of service class.
    pub qos: Option<MqttQos>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a range of data as a closed arithmetic interval of offsets
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OffsetInterval {
    /// Start of the closed interval [start; end].
    pub start: u64,
    /// End of the closed interval [start; end].
    pub end: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

impl_enum_with_variants!(PrepStep);

/// Pulls data from one of the supported sources by its URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepDecompress {
    /// Name of a compression algorithm used on data.
    pub format: CompressionFormat,
    /// Path to a data file within a multi-file archive. Can contain glob
    /// patterns.
    pub sub_path: Option<String>,
}

impl_enum_variant!(PrepStep::Decompress(PrepStepDecompress));

/// Executes external command to process the data using piped input/output.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepPipe {
    /// Command to execute and its arguments.
    pub command: Vec<String>,
}

impl_enum_variant!(PrepStep::Pipe(PrepStepPipe));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform query on raw input data,
/// usually as part of ingest preprocessing step
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryRequest {
    /// Paths to input data files to perform query over. Must all have identical
    /// schema.
    pub input_data_paths: Vec<PathBuf>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
    /// Path where query result will be written.
    pub output_data_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RawQueryResponse {
    Progress(RawQueryResponseProgress),
    Success(RawQueryResponseSuccess),
    InvalidQuery(RawQueryResponseInvalidQuery),
    InternalError(RawQueryResponseInternalError),
}

impl_enum_with_variants!(RawQueryResponse);

/// Reports query progress
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseProgress {}

impl_enum_variant!(RawQueryResponse::Progress(RawQueryResponseProgress));

/// Query executed successfully
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseSuccess {
    /// Number of records produced by the query
    pub num_records: u64,
}

impl_enum_variant!(RawQueryResponse::Success(RawQueryResponseSuccess));

/// Query did not pass validation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl_enum_variant!(RawQueryResponse::InvalidQuery(RawQueryResponseInvalidQuery));

/// Internal error during query execution
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl_enum_variant!(RawQueryResponse::InternalError(
    RawQueryResponseInternalError
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ReadStep {
    Csv(ReadStepCsv),
    GeoJson(ReadStepGeoJson),
    EsriShapefile(ReadStepEsriShapefile),
    Parquet(ReadStepParquet),
    Json(ReadStepJson),
    NdJson(ReadStepNdJson),
    NdGeoJson(ReadStepNdGeoJson),
}

impl_enum_with_variants!(ReadStep);

/// Reader for comma-separated files.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepCsv {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets a single character as a separator for each field and value.
    pub separator: Option<String>,
    /// Decodes the CSV files by the given encoding type.
    pub encoding: Option<String>,
    /// Sets a single character used for escaping quoted values where the
    /// separator can be part of the value. Set an empty string to turn off
    /// quotations.
    pub quote: Option<String>,
    /// Sets a single character used for escaping quotes inside an already
    /// quoted value.
    pub escape: Option<String>,
    /// Use the first line as names of columns.
    pub header: Option<bool>,
    /// Infers the input schema automatically from data. It requires one extra
    /// pass over the data.
    pub infer_schema: Option<bool>,
    /// Sets the string representation of a null value.
    pub null_value: Option<String>,
    /// Sets the string that indicates a date format. The `rfc3339` is the only
    /// required format, the other format strings are implementation-specific.
    pub date_format: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    pub timestamp_format: Option<String>,
}

impl_enum_variant!(ReadStep::Csv(ReadStepCsv));

/// Reader for GeoJSON files. It expects one `FeatureCollection` object in the
/// root and will create a record per each `Feature` inside it extracting the
/// properties into individual columns and leaving the feature geometry in its
/// own column.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl_enum_variant!(ReadStep::GeoJson(ReadStepGeoJson));

/// Reader for ESRI Shapefile format.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepEsriShapefile {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// If the ZIP archive contains multiple shapefiles use this field to
    /// specify a sub-path to the desired `.shp` file. Can contain glob patterns
    /// to act as a filter.
    pub sub_path: Option<String>,
}

impl_enum_variant!(ReadStep::EsriShapefile(ReadStepEsriShapefile));

/// Reader for Apache Parquet format.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepParquet {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl_enum_variant!(ReadStep::Parquet(ReadStepParquet));

/// Reader for JSON files that contain an array of objects within them.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepJson {
    /// Path in the form of `a.b.c` to a sub-element of the root JSON object
    /// that is an array or objects. If not specified it is assumed that the
    /// root element is an array.
    pub sub_path: Option<String>,
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets the string that indicates a date format. The `rfc3339` is the only
    /// required format, the other format strings are implementation-specific.
    pub date_format: Option<String>,
    /// Allows to forcibly set one of standard basic or extended encodings.
    pub encoding: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    pub timestamp_format: Option<String>,
}

impl_enum_variant!(ReadStep::Json(ReadStepJson));

/// Reader for files containing multiple newline-delimited JSON objects with the
/// same schema.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepNdJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets the string that indicates a date format. The `rfc3339` is the only
    /// required format, the other format strings are implementation-specific.
    pub date_format: Option<String>,
    /// Allows to forcibly set one of standard basic or extended encodings.
    pub encoding: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    pub timestamp_format: Option<String>,
}

impl_enum_variant!(ReadStep::NdJson(ReadStepNdJson));

/// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson`
/// format but instead of `FeatureCollection` object in the root it expects
/// every individual feature object to appear on its own line.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepNdGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl_enum_variant!(ReadStep::NdGeoJson(ReadStepNdGeoJson));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a header (e.g. HTTP) to be passed into some request.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RequestHeader {
    /// Name of the header.
    pub name: String,
    /// Value of the header.
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Establishes the identity of the dataset. Always the first metadata event in
/// the chain.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seed {
    /// Unique identity of the dataset.
    pub dataset_id: DatasetID,
    /// Type of the dataset.
    pub dataset_kind: DatasetKind,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Associates a set of files with this dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetAttachments {
    /// One of the supported attachment sources.
    pub attachments: Attachments,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the complete schema of Data Slices added to the Dataset following
/// this event.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetDataSchema {
    /// Apache Arrow schema encoded in its native flatbuffers representation.
    pub schema: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides basic human-readable information about a dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetInfo {
    /// Brief single-sentence summary of a dataset.
    pub description: Option<String>,
    /// Keywords, search terms, or tags used to describe the dataset.
    pub keywords: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a license that applies to this dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetLicense {
    /// Abbreviated name of the license.
    pub short_name: String,
    /// Full name of the license.
    pub name: String,
    /// License identifier from the SPDX License List.
    pub spdx_id: Option<String>,
    /// URL where licensing terms can be found.
    pub website_url: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains information on how externally-hosted data can be ingested into the
/// root dataset.
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
    /// Determines how newly-ingested data should be merged with existing
    /// history.
    pub merge: MergeStrategy,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a transformation that produces data in a derivative dataset.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetTransform {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lets you manipulate names of the system columns to avoid conflicts.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetVocab {
    /// Name of the offset column.
    pub offset_column: Option<String>,
    /// Name of the operation type column.
    pub operation_type_column: Option<String>,
    /// Name of the system time column.
    pub system_time_column: Option<String>,
    /// Name of the event time column.
    pub event_time_column: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SourceCaching {
    Forever(SourceCachingForever),
}

impl_enum_with_variants!(SourceCaching);

/// After source was processed once it will never be ingested again.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SourceCachingForever {}

impl_enum_variant!(SourceCaching::Forever(SourceCachingForever));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceOrdering
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the source the data was added from to allow fast resuming.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SourceState {
    /// Identifies the source that the state corresponds to.
    pub source_name: String,
    /// Identifies the type of the state. Standard types include: `odf/etag`,
    /// `odf/last-modified`.
    pub kind: String,
    /// Opaque value representing the state.
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a query in a multi-step SQL transformation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SqlQueryStep {
    /// Name of the temporary view that will be created from result of the
    /// query. Step without this alias will be treated as an output of the
    /// transformation.
    pub alias: Option<String>,
    /// SQL query the result of which will be exposed under the alias.
    pub query: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Temporary Flink-specific extension for creating temporal tables from
/// streams.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TemporalTable {
    /// Name of the dataset to be converted into a temporal table.
    pub name: String,
    /// Column names used as the primary key for creating a table.
    pub primary_key: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Transform {
    Sql(TransformSql),
}

impl_enum_with_variants!(Transform);

/// Transform using one of the SQL dialects.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformSql {
    /// Identifier of the engine used for this transformation.
    pub engine: String,
    /// Version of the engine to use.
    pub version: Option<String>,
    /// SQL query the result of which will be used as an output. This is a
    /// convenience property meant only for defining queries by hand. When
    /// stored in the metadata this property will never be set and instead will
    /// be converted into a single-iter `queries` array.
    pub query: Option<String>,
    /// Specifies multi-step SQL transformations. Each step acts as a shorthand
    /// for `CREATE TEMPORARY VIEW <alias> AS (<query>)`. Last query in the
    /// array should have no alias and will be treated as an output.
    pub queries: Option<Vec<SqlQueryStep>>,
    /// Temporary Flink-specific extension for creating temporal tables from
    /// streams.
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

impl_enum_variant!(Transform::Sql(TransformSql));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a derivative transformation input
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformInput {
    /// A local or remote dataset reference. When block is accepted this MUST be
    /// in the form of a DatasetId to guarantee reproducibility, as aliases can
    /// change over time.
    pub dataset_ref: DatasetRef,
    /// An alias under which this input will be available in queries. Will be
    /// populated from `datasetRef` if not provided before resolving it to
    /// DatasetId.
    pub alias: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform the next step of data
/// transformation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformRequest {
    /// Unique identifier of the output dataset.
    pub dataset_id: DatasetID,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias,
    /// System time to use for new records.
    pub system_time: DateTime<Utc>,
    pub vocab: DatasetVocabulary,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
    /// Defines inputs used in this transaction. Slices corresponding to every
    /// input dataset must be present.
    pub query_inputs: Vec<TransformRequestInput>,
    /// Starting offset to use for new data records.
    pub next_offset: u64,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub prev_checkpoint_path: Option<PathBuf>,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_checkpoint_path: PathBuf,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_data_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent as part of the engine transform request operation to describe the input
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformRequestInput {
    /// Unique identifier of the dataset.
    pub dataset_id: DatasetID,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias,
    /// An alias of this input to be used in queries.
    pub query_alias: String,
    pub vocab: DatasetVocabulary,
    /// Subset of data that goes into this transaction.
    pub offset_interval: Option<OffsetInterval>,
    /// TODO: This will be removed when coordinator will be slicing data for the
    /// engine.
    pub data_paths: Vec<PathBuf>,
    /// TODO: replace with actual DDL or Parquet schema.
    pub schema_file: PathBuf,
    /// Watermarks that should be injected into the stream to separate micro
    /// batches for reproducibility.
    pub explicit_watermarks: Vec<Watermark>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TransformResponse {
    Progress(TransformResponseProgress),
    Success(TransformResponseSuccess),
    InvalidQuery(TransformResponseInvalidQuery),
    InternalError(TransformResponseInternalError),
}

impl_enum_with_variants!(TransformResponse);

/// Reports query progress
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseProgress {}

impl_enum_variant!(TransformResponse::Progress(TransformResponseProgress));

/// Query executed successfully
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseSuccess {
    /// Data slice produced by the transaction, if any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any.
    pub new_watermark: Option<DateTime<Utc>>,
}

impl_enum_variant!(TransformResponse::Success(TransformResponseSuccess));

/// Query did not pass validation
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl_enum_variant!(TransformResponse::InvalidQuery(
    TransformResponseInvalidQuery
));

/// Internal error during query execution
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl_enum_variant!(TransformResponse::InternalError(
    TransformResponseInternalError
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a watermark in the event stream.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Watermark {
    /// Moment in processing time when watermark was emitted.
    pub system_time: DateTime<Utc>,
    /// Moment in event time which watermark has reached.
    pub event_time: DateTime<Utc>,
}
