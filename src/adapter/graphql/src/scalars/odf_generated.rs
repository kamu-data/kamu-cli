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

#![allow(unused_variables)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use chrono::{DateTime, Utc};

use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that data has been ingested into a root dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AddData {
    /// Hash of the checkpoint file used to restore ingestion state, if any.
    pub prev_checkpoint: Option<Multihash<'static>>,
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

impl From<odf::metadata::AddData> for AddData {
    fn from(v: odf::metadata::AddData) -> Self {
        Self {
            prev_checkpoint: v.prev_checkpoint.map(Into::into),
            prev_offset: v.prev_offset.map(Into::into),
            new_data: v.new_data.map(Into::into),
            new_checkpoint: v.new_checkpoint.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
            new_source_state: v.new_source_state.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes how to ingest data into a root dataset from a certain logical
/// source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::AddPushSource> for AddPushSource {
    fn from(v: odf::metadata::AddPushSource) -> Self {
        Self {
            source_name: v.source_name.into(),
            read: v.read.into(),
            preprocess: v.preprocess.map(Into::into),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Embedded attachment item.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentEmbedded {
    /// Path to an attachment if it was materialized into a file.
    pub path: String,
    /// Content of the attachment.
    pub content: String,
}

impl From<odf::metadata::AttachmentEmbedded> for AttachmentEmbedded {
    fn from(v: odf::metadata::AttachmentEmbedded) -> Self {
        Self {
            path: v.path.into(),
            content: v.content.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the source of attachment files.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

impl From<odf::metadata::Attachments> for Attachments {
    fn from(v: odf::metadata::Attachments) -> Self {
        match v {
            odf::metadata::Attachments::Embedded(v) => Self::Embedded(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// For attachments that are specified inline and are embedded in the metadata.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentsembedded-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentsEmbedded {
    /// List of embedded items.
    pub items: Vec<AttachmentEmbedded>,
}

impl From<odf::metadata::AttachmentsEmbedded> for AttachmentsEmbedded {
    fn from(v: odf::metadata::AttachmentsEmbedded) -> Self {
        Self {
            items: v.items.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a checkpoint produced by an engine
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Hash sum of the checkpoint file.
    pub physical_hash: Multihash<'static>,
    /// Size of checkpoint file in bytes.
    pub size: u64,
}

impl From<odf::metadata::Checkpoint> for Checkpoint {
    fn from(v: odf::metadata::Checkpoint) -> Self {
        Self {
            physical_hash: v.physical_hash.into(),
            size: v.size.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a compression algorithm.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    Gzip,
    Zip,
}

impl From<odf::metadata::CompressionFormat> for CompressionFormat {
    fn from(v: odf::metadata::CompressionFormat) -> Self {
        match v {
            odf::metadata::CompressionFormat::Gzip => Self::Gzip,
            odf::metadata::CompressionFormat::Zip => Self::Zip,
        }
    }
}

impl Into<odf::metadata::CompressionFormat> for CompressionFormat {
    fn into(self) -> odf::metadata::CompressionFormat {
        match self {
            Self::Gzip => odf::metadata::CompressionFormat::Gzip,
            Self::Zip => odf::metadata::CompressionFormat::Zip,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of data added to a dataset or produced via transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DataSlice {
    /// Logical hash sum of the data in this slice.
    pub logical_hash: Multihash<'static>,
    /// Hash sum of the data part file.
    pub physical_hash: Multihash<'static>,
    /// Data slice produced by the transaction.
    pub offset_interval: OffsetInterval,
    /// Size of data file in bytes.
    pub size: u64,
}

impl From<odf::metadata::DataSlice> for DataSlice {
    fn from(v: odf::metadata::DataSlice) -> Self {
        Self {
            logical_hash: v.logical_hash.into(),
            physical_hash: v.physical_hash.into(),
            offset_interval: v.offset_interval.into(),
            size: v.size.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents type of the dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetKind {
    Root,
    Derivative,
}

impl From<odf::metadata::DatasetKind> for DatasetKind {
    fn from(v: odf::metadata::DatasetKind) -> Self {
        match v {
            odf::metadata::DatasetKind::Root => Self::Root,
            odf::metadata::DatasetKind::Derivative => Self::Derivative,
        }
    }
}

impl Into<odf::metadata::DatasetKind> for DatasetKind {
    fn into(self) -> odf::metadata::DatasetKind {
        match self {
            Self::Root => odf::metadata::DatasetKind::Root,
            Self::Derivative => odf::metadata::DatasetKind::Derivative,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a projection of the dataset metadata at a single point in time.
/// This type is typically used for defining new datasets and changing the
/// existing ones.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshot {
    /// Alias of the dataset.
    pub name: DatasetAlias<'static>,
    /// Type of the dataset.
    pub kind: DatasetKind,
    /// An array of metadata events that will be used to populate the chain.
    /// Here you can define polling and push sources, set licenses, add
    /// attachments etc.
    pub metadata: Vec<MetadataEvent>,
}

impl From<odf::metadata::DatasetSnapshot> for DatasetSnapshot {
    fn from(v: odf::metadata::DatasetSnapshot) -> Self {
        Self {
            name: v.name.into(),
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the mapping of system columns onto dataset schema.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetVocabulary {
    /// Name of the offset column.
    ///
    /// Defaults to: "offset"
    pub offset_column: String,
    /// Name of the operation type column.
    ///
    /// Defaults to: "op"
    pub operation_type_column: String,
    /// Name of the system time column.
    ///
    /// Defaults to: "system_time"
    pub system_time_column: String,
    /// Name of the event time column.
    ///
    /// Defaults to: "event_time"
    pub event_time_column: String,
}

impl From<odf::metadata::DatasetVocabulary> for DatasetVocabulary {
    fn from(v: odf::metadata::DatasetVocabulary) -> Self {
        Self {
            offset_column: v.offset_column.into(),
            operation_type_column: v.operation_type_column.into(),
            system_time_column: v.system_time_column.into(),
            event_time_column: v.event_time_column.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined polling source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DisablePollingSource {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::DisablePollingSource> for DisablePollingSource {
    fn from(v: odf::metadata::DisablePollingSource) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DisablePushSource {
    /// Identifies the source to be disabled.
    pub source_name: String,
}

impl From<odf::metadata::DisablePushSource> for DisablePushSource {
    fn from(v: odf::metadata::DisablePushSource) -> Self {
        Self {
            source_name: v.source_name.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines an environment variable passed into some job.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EnvVar {
    /// Name of the variable.
    pub name: String,
    /// Value of the variable.
    pub value: Option<String>,
}

impl From<odf::metadata::EnvVar> for EnvVar {
    fn from(v: odf::metadata::EnvVar) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum EventTimeSource {
    FromMetadata(EventTimeSourceFromMetadata),
    FromPath(EventTimeSourceFromPath),
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl From<odf::metadata::EventTimeSource> for EventTimeSource {
    fn from(v: odf::metadata::EventTimeSource) -> Self {
        match v {
            odf::metadata::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
            odf::metadata::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
            odf::metadata::EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the source's metadata.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrommetadata-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromMetadata {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
    fn from(v: odf::metadata::EventTimeSourceFromMetadata) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the path component of the source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrompath-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromPath {
    /// Regular expression where first group contains the timestamp string.
    pub pattern: String,
    /// Format of the expected timestamp in java.text.SimpleDateFormat form.
    pub timestamp_format: Option<String>,
}

impl From<odf::metadata::EventTimeSourceFromPath> for EventTimeSourceFromPath {
    fn from(v: odf::metadata::EventTimeSourceFromPath) -> Self {
        Self {
            pattern: v.pattern.into(),
            timestamp_format: v.timestamp_format.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assigns event time from the system time source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefromsystemtime-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromSystemTime {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
    fn from(v: odf::metadata::EventTimeSourceFromSystemTime) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that derivative transformation has been performed.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTransform {
    /// Defines inputs used in this transaction. Slices corresponding to every
    /// input dataset must be present.
    pub query_inputs: Vec<ExecuteTransformInput>,
    /// Hash of the checkpoint file used to restore transformation state, if
    /// any.
    pub prev_checkpoint: Option<Multihash<'static>>,
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

impl From<odf::metadata::ExecuteTransform> for ExecuteTransform {
    fn from(v: odf::metadata::ExecuteTransform) -> Self {
        Self {
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            prev_checkpoint: v.prev_checkpoint.map(Into::into),
            prev_offset: v.prev_offset.map(Into::into),
            new_data: v.new_data.map(Into::into),
            new_checkpoint: v.new_checkpoint.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of the input dataset used during a transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTransformInput {
    /// Input dataset identifier.
    pub dataset_id: DatasetID<'static>,
    /// Last block of the input dataset that was previously incorporated into
    /// the derivative transformation, if any. Must be equal to the last
    /// non-empty `newBlockHash`. Together with `newBlockHash` defines a
    /// half-open `(prevBlockHash, newBlockHash]` interval of blocks that will
    /// be considered in this transaction.
    pub prev_block_hash: Option<Multihash<'static>>,
    /// Hash of the last block that will be incorporated into the derivative
    /// transformation. When present, defines a half-open `(prevBlockHash,
    /// newBlockHash]` interval of blocks that will be considered in this
    /// transaction.
    pub new_block_hash: Option<Multihash<'static>>,
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

impl From<odf::metadata::ExecuteTransformInput> for ExecuteTransformInput {
    fn from(v: odf::metadata::ExecuteTransformInput) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            prev_block_hash: v.prev_block_hash.map(Into::into),
            new_block_hash: v.new_block_hash.map(Into::into),
            prev_offset: v.prev_offset.map(Into::into),
            new_offset: v.new_offset.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
    Mqtt(FetchStepMqtt),
    EthereumLogs(FetchStepEthereumLogs),
}

impl From<odf::metadata::FetchStep> for FetchStep {
    fn from(v: odf::metadata::FetchStep) -> Self {
        match v {
            odf::metadata::FetchStep::Url(v) => Self::Url(v.into()),
            odf::metadata::FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
            odf::metadata::FetchStep::Container(v) => Self::Container(v.into()),
            odf::metadata::FetchStep::Mqtt(v) => Self::Mqtt(v.into()),
            odf::metadata::FetchStep::EthereumLogs(v) => Self::EthereumLogs(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the specified OCI container to fetch data from an arbitrary source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepcontainer-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::FetchStepContainer> for FetchStepContainer {
    fn from(v: odf::metadata::FetchStepContainer) -> Self {
        Self {
            image: v.image.into(),
            command: v.command.map(|v| v.into_iter().map(Into::into).collect()),
            args: v.args.map(|v| v.into_iter().map(Into::into).collect()),
            env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Connects to an Ethereum node to stream transaction logs.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepethereumlogs-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepEthereumLogs {
    /// Identifier of the chain to scan logs from. This parameter may be used
    /// for RPC endpoint lookup as well as asserting that provided `nodeUrl`
    /// corresponds to the expected chain.
    pub chain_id: Option<u64>,
    /// Url of the node.
    pub node_url: Option<String>,
    /// An SQL WHERE clause that can be used to pre-filter the logs before
    /// fetching them from the ETH node.
    ///
    /// Examples:
    /// - "block_number > 123 and address =
    ///   X'5fbdb2315678afecb367f032d93f642f64180aa3' and topic1 =
    ///   X'000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'"
    pub filter: Option<String>,
    /// Solidity log event signature to use for decoding. Using this field adds
    /// `event` to the output containing decoded log as JSON.
    pub signature: Option<String>,
}

impl From<odf::metadata::FetchStepEthereumLogs> for FetchStepEthereumLogs {
    fn from(v: odf::metadata::FetchStepEthereumLogs) -> Self {
        Self {
            chain_id: v.chain_id.map(Into::into),
            node_url: v.node_url.map(Into::into),
            filter: v.filter.map(Into::into),
            signature: v.signature.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uses glob operator to match files on the local file system.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepfilesglob-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::FetchStepFilesGlob> for FetchStepFilesGlob {
    fn from(v: odf::metadata::FetchStepFilesGlob) -> Self {
        Self {
            path: v.path.into(),
            event_time: v.event_time.map(Into::into),
            cache: v.cache.map(Into::into),
            order: v.order.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Connects to an MQTT broker to fetch events from the specified topic.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepmqtt-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::FetchStepMqtt> for FetchStepMqtt {
    fn from(v: odf::metadata::FetchStepMqtt) -> Self {
        Self {
            host: v.host.into(),
            port: v.port.into(),
            username: v.username.map(Into::into),
            password: v.password.map(Into::into),
            topics: v.topics.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepurl-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::FetchStepUrl> for FetchStepUrl {
    fn from(v: odf::metadata::FetchStepUrl) -> Self {
        Self {
            url: v.url.into(),
            event_time: v.event_time.map(Into::into),
            cache: v.cache.map(Into::into),
            headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Merge strategy determines how newly ingested data should be combined with
/// the data that already exists in the dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
    ChangelogStream(MergeStrategyChangelogStream),
    UpsertStream(MergeStrategyUpsertStream),
}

impl From<odf::metadata::MergeStrategy> for MergeStrategy {
    fn from(v: odf::metadata::MergeStrategy) -> Self {
        match v {
            odf::metadata::MergeStrategy::Append(v) => Self::Append(v.into()),
            odf::metadata::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            odf::metadata::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
            odf::metadata::MergeStrategy::ChangelogStream(v) => Self::ChangelogStream(v.into()),
            odf::metadata::MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Append merge strategy.
///
/// Under this strategy new data will be appended to the dataset in its
/// entirety, without any deduplication.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyappend-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyAppend {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::MergeStrategyAppend> for MergeStrategyAppend {
    fn from(v: odf::metadata::MergeStrategyAppend) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Changelog stream merge strategy.
///
/// This is the native stream format for ODF that accurately describes the
/// evolution of all event records including appends, retractions, and
/// corrections as per RFC-015. No pre-processing except for format validation
/// is done.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategychangelogstream-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyChangelogStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::MergeStrategyChangelogStream> for MergeStrategyChangelogStream {
    fn from(v: odf::metadata::MergeStrategyChangelogStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Ledger merge strategy.
///
/// This strategy should be used for data sources containing ledgers of events.
/// Currently this strategy will only perform deduplication of events using
/// user-specified primary key columns. This means that the source data can
/// contain partially overlapping set of records and only those records that
/// were not previously seen will be appended.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyledger-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyLedger {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::MergeStrategyLedger> for MergeStrategyLedger {
    fn from(v: odf::metadata::MergeStrategyLedger) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
///   - append (`+A`) when a row appears for the first time
///   - retraction (`-D`) when row disappears
///   - correction (`-C`, `+C`) when row data has changed, with `-C` event
///     carrying the old value of the row and `+C` carrying the new value.
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
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategysnapshot-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategySnapshot {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime.
    pub primary_key: Vec<String>,
    /// Names of the columns to compared to determine if a row has changed
    /// between two snapshots.
    pub compare_columns: Option<Vec<String>>,
}

impl From<odf::metadata::MergeStrategySnapshot> for MergeStrategySnapshot {
    fn from(v: odf::metadata::MergeStrategySnapshot) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
            compare_columns: v
                .compare_columns
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Upsert stream merge strategy.
///
/// This strategy should be used for data sources containing ledgers of
/// insert-or-update and delete events. Unlike ChangelogStream the
/// insert-or-update events only carry the new values, so this strategy will use
/// primary key to re-classify the events into an append or a correction from/to
/// pair, looking up the previous values.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyupsertstream-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyUpsertStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::MergeStrategyUpsertStream> for MergeStrategyUpsertStream {
    fn from(v: odf::metadata::MergeStrategyUpsertStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An individual block in the metadata chain that captures the history of
/// modifications of a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlock {
    /// System time when this block was written.
    pub system_time: DateTime<Utc>,
    /// Hash sum of the preceding block.
    pub prev_block_hash: Option<Multihash<'static>>,
    /// Block sequence number, starting from zero at the seed block.
    pub sequence_number: u64,
    /// Event data.
    pub event: MetadataEvent,
}

impl From<odf::metadata::MetadataBlock> for MetadataBlock {
    fn from(v: odf::metadata::MetadataBlock) -> Self {
        Self {
            system_time: v.system_time.into(),
            prev_block_hash: v.prev_block_hash.map(Into::into),
            sequence_number: v.sequence_number.into(),
            event: v.event.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a transaction that occurred on a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::MetadataEvent> for MetadataEvent {
    fn from(v: odf::metadata::MetadataEvent) -> Self {
        match v {
            odf::metadata::MetadataEvent::AddData(v) => Self::AddData(v.into()),
            odf::metadata::MetadataEvent::ExecuteTransform(v) => Self::ExecuteTransform(v.into()),
            odf::metadata::MetadataEvent::Seed(v) => Self::Seed(v.into()),
            odf::metadata::MetadataEvent::SetPollingSource(v) => Self::SetPollingSource(v.into()),
            odf::metadata::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
            odf::metadata::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
            odf::metadata::MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
            odf::metadata::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
            odf::metadata::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
            odf::metadata::MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
            odf::metadata::MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
            odf::metadata::MetadataEvent::DisablePushSource(v) => Self::DisablePushSource(v.into()),
            odf::metadata::MetadataEvent::DisablePollingSource(v) => {
                Self::DisablePollingSource(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT quality of service class.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl From<odf::metadata::MqttQos> for MqttQos {
    fn from(v: odf::metadata::MqttQos) -> Self {
        match v {
            odf::metadata::MqttQos::AtMostOnce => Self::AtMostOnce,
            odf::metadata::MqttQos::AtLeastOnce => Self::AtLeastOnce,
            odf::metadata::MqttQos::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

impl Into<odf::metadata::MqttQos> for MqttQos {
    fn into(self) -> odf::metadata::MqttQos {
        match self {
            Self::AtMostOnce => odf::metadata::MqttQos::AtMostOnce,
            Self::AtLeastOnce => odf::metadata::MqttQos::AtLeastOnce,
            Self::ExactlyOnce => odf::metadata::MqttQos::ExactlyOnce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT topic subscription parameters.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MqttTopicSubscription {
    /// Name of the topic (may include patterns).
    pub path: String,
    /// Quality of service class.
    ///
    /// Defaults to: "AtMostOnce"
    pub qos: Option<MqttQos>,
}

impl From<odf::metadata::MqttTopicSubscription> for MqttTopicSubscription {
    fn from(v: odf::metadata::MqttTopicSubscription) -> Self {
        Self {
            path: v.path.into(),
            qos: v.qos.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a range of data as a closed arithmetic interval of offsets
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct OffsetInterval {
    /// Start of the closed interval [start; end].
    pub start: u64,
    /// End of the closed interval [start; end].
    pub end: u64,
}

impl From<odf::metadata::OffsetInterval> for OffsetInterval {
    fn from(v: odf::metadata::OffsetInterval) -> Self {
        Self {
            start: v.start.into(),
            end: v.end.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the steps to prepare raw data for ingestion.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

impl From<odf::metadata::PrepStep> for PrepStep {
    fn from(v: odf::metadata::PrepStep) -> Self {
        match v {
            odf::metadata::PrepStep::Decompress(v) => Self::Decompress(v.into()),
            odf::metadata::PrepStep::Pipe(v) => Self::Pipe(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstepdecompress-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct PrepStepDecompress {
    /// Name of a compression algorithm used on data.
    pub format: CompressionFormat,
    /// Path to a data file within a multi-file archive. Can contain glob
    /// patterns.
    pub sub_path: Option<String>,
}

impl From<odf::metadata::PrepStepDecompress> for PrepStepDecompress {
    fn from(v: odf::metadata::PrepStepDecompress) -> Self {
        Self {
            format: v.format.into(),
            sub_path: v.sub_path.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Executes external command to process the data using piped input/output.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepsteppipe-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct PrepStepPipe {
    /// Command to execute and its arguments.
    pub command: Vec<String>,
}

impl From<odf::metadata::PrepStepPipe> for PrepStepPipe {
    fn from(v: odf::metadata::PrepStepPipe) -> Self {
        Self {
            command: v.command.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform query on raw input data,
/// usually as part of ingest preprocessing step
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryRequest {
    /// Paths to input data files to perform query over. Must all have identical
    /// schema.
    pub input_data_paths: Vec<OSPath>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
    /// Path where query result will be written.
    pub output_data_path: OSPath,
}

impl From<odf::metadata::RawQueryRequest> for RawQueryRequest {
    fn from(v: odf::metadata::RawQueryRequest) -> Self {
        Self {
            input_data_paths: v.input_data_paths.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
            output_data_path: v.output_data_path.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by an engine to coordinator when performing the raw query operation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum RawQueryResponse {
    Progress(RawQueryResponseProgress),
    Success(RawQueryResponseSuccess),
    InvalidQuery(RawQueryResponseInvalidQuery),
    InternalError(RawQueryResponseInternalError),
}

impl From<odf::metadata::RawQueryResponse> for RawQueryResponse {
    fn from(v: odf::metadata::RawQueryResponse) -> Self {
        match v {
            odf::metadata::RawQueryResponse::Progress(v) => Self::Progress(v.into()),
            odf::metadata::RawQueryResponse::Success(v) => Self::Success(v.into()),
            odf::metadata::RawQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            odf::metadata::RawQueryResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinternalerror-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl From<odf::metadata::RawQueryResponseInternalError> for RawQueryResponseInternalError {
    fn from(v: odf::metadata::RawQueryResponseInternalError) -> Self {
        Self {
            message: v.message.into(),
            backtrace: v.backtrace.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinvalidquery-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl From<odf::metadata::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
    fn from(v: odf::metadata::RawQueryResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseprogress-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::RawQueryResponseProgress> for RawQueryResponseProgress {
    fn from(v: odf::metadata::RawQueryResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponsesuccess-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseSuccess {
    /// Number of records produced by the query
    pub num_records: u64,
}

impl From<odf::metadata::RawQueryResponseSuccess> for RawQueryResponseSuccess {
    fn from(v: odf::metadata::RawQueryResponseSuccess) -> Self {
        Self {
            num_records: v.num_records.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines how raw data should be read into the structured form.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum ReadStep {
    Csv(ReadStepCsv),
    GeoJson(ReadStepGeoJson),
    EsriShapefile(ReadStepEsriShapefile),
    Parquet(ReadStepParquet),
    Json(ReadStepJson),
    NdJson(ReadStepNdJson),
    NdGeoJson(ReadStepNdGeoJson),
}

impl From<odf::metadata::ReadStep> for ReadStep {
    fn from(v: odf::metadata::ReadStep) -> Self {
        match v {
            odf::metadata::ReadStep::Csv(v) => Self::Csv(v.into()),
            odf::metadata::ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
            odf::metadata::ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
            odf::metadata::ReadStep::Parquet(v) => Self::Parquet(v.into()),
            odf::metadata::ReadStep::Json(v) => Self::Json(v.into()),
            odf::metadata::ReadStep::NdJson(v) => Self::NdJson(v.into()),
            odf::metadata::ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for comma-separated files.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepcsv-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepCsv {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    ///
    /// Examples:
    /// - ["date TIMESTAMP","city STRING","population INT"]
    pub schema: Option<Vec<String>>,
    /// Sets a single character as a separator for each field and value.
    ///
    /// Defaults to: ","
    pub separator: Option<String>,
    /// Decodes the CSV files by the given encoding type.
    ///
    /// Defaults to: "utf8"
    pub encoding: Option<String>,
    /// Sets a single character used for escaping quoted values where the
    /// separator can be part of the value. Set an empty string to turn off
    /// quotations.
    ///
    /// Defaults to: "\""
    pub quote: Option<String>,
    /// Sets a single character used for escaping quotes inside an already
    /// quoted value.
    ///
    /// Defaults to: "\\"
    pub escape: Option<String>,
    /// Use the first line as names of columns.
    ///
    /// Defaults to: false
    pub header: Option<bool>,
    /// Infers the input schema automatically from data. It requires one extra
    /// pass over the data.
    ///
    /// Defaults to: false
    pub infer_schema: Option<bool>,
    /// Sets the string representation of a null value.
    ///
    /// Defaults to: ""
    pub null_value: Option<String>,
    /// Sets the string that indicates a date format. The `rfc3339` is the only
    /// required format, the other format strings are implementation-specific.
    ///
    /// Defaults to: "rfc3339"
    pub date_format: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    ///
    /// Defaults to: "rfc3339"
    pub timestamp_format: Option<String>,
}

impl From<odf::metadata::ReadStepCsv> for ReadStepCsv {
    fn from(v: odf::metadata::ReadStepCsv) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
            separator: v.separator.map(Into::into),
            encoding: v.encoding.map(Into::into),
            quote: v.quote.map(Into::into),
            escape: v.escape.map(Into::into),
            header: v.header.map(Into::into),
            infer_schema: v.infer_schema.map(Into::into),
            null_value: v.null_value.map(Into::into),
            date_format: v.date_format.map(Into::into),
            timestamp_format: v.timestamp_format.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for ESRI Shapefile format.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepesrishapefile-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepEsriShapefile {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// If the ZIP archive contains multiple shapefiles use this field to
    /// specify a sub-path to the desired `.shp` file. Can contain glob patterns
    /// to act as a filter.
    pub sub_path: Option<String>,
}

impl From<odf::metadata::ReadStepEsriShapefile> for ReadStepEsriShapefile {
    fn from(v: odf::metadata::ReadStepEsriShapefile) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
            sub_path: v.sub_path.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for GeoJSON files. It expects one `FeatureCollection` object in the
/// root and will create a record per each `Feature` inside it extracting the
/// properties into individual columns and leaving the feature geometry in its
/// own column.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepgeojson-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl From<odf::metadata::ReadStepGeoJson> for ReadStepGeoJson {
    fn from(v: odf::metadata::ReadStepGeoJson) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for JSON files that contain an array of objects within them.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepjson-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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
    ///
    /// Defaults to: "rfc3339"
    pub date_format: Option<String>,
    /// Allows to forcibly set one of standard basic or extended encodings.
    ///
    /// Defaults to: "utf8"
    pub encoding: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    ///
    /// Defaults to: "rfc3339"
    pub timestamp_format: Option<String>,
}

impl From<odf::metadata::ReadStepJson> for ReadStepJson {
    fn from(v: odf::metadata::ReadStepJson) -> Self {
        Self {
            sub_path: v.sub_path.map(Into::into),
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(Into::into),
            encoding: v.encoding.map(Into::into),
            timestamp_format: v.timestamp_format.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson`
/// format but instead of `FeatureCollection` object in the root it expects
/// every individual feature object to appear on its own line.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndgeojson-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepNdGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl From<odf::metadata::ReadStepNdGeoJson> for ReadStepNdGeoJson {
    fn from(v: odf::metadata::ReadStepNdGeoJson) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for files containing multiple newline-delimited JSON objects with the
/// same schema.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndjson-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepNdJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
    /// Sets the string that indicates a date format. The `rfc3339` is the only
    /// required format, the other format strings are implementation-specific.
    ///
    /// Defaults to: "rfc3339"
    pub date_format: Option<String>,
    /// Allows to forcibly set one of standard basic or extended encodings.
    ///
    /// Defaults to: "utf8"
    pub encoding: Option<String>,
    /// Sets the string that indicates a timestamp format. The `rfc3339` is the
    /// only required format, the other format strings are
    /// implementation-specific.
    ///
    /// Defaults to: "rfc3339"
    pub timestamp_format: Option<String>,
}

impl From<odf::metadata::ReadStepNdJson> for ReadStepNdJson {
    fn from(v: odf::metadata::ReadStepNdJson) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
            date_format: v.date_format.map(Into::into),
            encoding: v.encoding.map(Into::into),
            timestamp_format: v.timestamp_format.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Apache Parquet format.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepparquet-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepParquet {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

impl From<odf::metadata::ReadStepParquet> for ReadStepParquet {
    fn from(v: odf::metadata::ReadStepParquet) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a header (e.g. HTTP) to be passed into some request.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    /// Name of the header.
    pub name: String,
    /// Value of the header.
    pub value: String,
}

impl From<odf::metadata::RequestHeader> for RequestHeader {
    fn from(v: odf::metadata::RequestHeader) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Establishes the identity of the dataset. Always the first metadata event in
/// the chain.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Seed {
    /// Unique identity of the dataset.
    pub dataset_id: DatasetID<'static>,
    /// Type of the dataset.
    pub dataset_kind: DatasetKind,
}

impl From<odf::metadata::Seed> for Seed {
    fn from(v: odf::metadata::Seed) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            dataset_kind: v.dataset_kind.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Associates a set of files with this dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetAttachments {
    /// One of the supported attachment sources.
    pub attachments: Attachments,
}

impl From<odf::metadata::SetAttachments> for SetAttachments {
    fn from(v: odf::metadata::SetAttachments) -> Self {
        Self {
            attachments: v.attachments.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the complete schema of Data Slices added to the Dataset following
/// this event.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetDataSchema {
    pub schema: DataSchema,
}

impl From<odf::metadata::SetDataSchema> for SetDataSchema {
    fn from(v: odf::metadata::SetDataSchema) -> Self {
        // TODO: Error handling?
        // TODO: Externalize format decision?
        let arrow_schema = v.schema_as_arrow().unwrap();
        let schema = DataSchema::from_arrow_schema(&arrow_schema, DataSchemaFormat::ParquetJson);
        Self { schema }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides basic human-readable information about a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetInfo {
    /// Brief single-sentence summary of a dataset.
    pub description: Option<String>,
    /// Keywords, search terms, or tags used to describe the dataset.
    pub keywords: Option<Vec<String>>,
}

impl From<odf::metadata::SetInfo> for SetInfo {
    fn from(v: odf::metadata::SetInfo) -> Self {
        Self {
            description: v.description.map(Into::into),
            keywords: v.keywords.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a license that applies to this dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::SetLicense> for SetLicense {
    fn from(v: odf::metadata::SetLicense) -> Self {
        Self {
            short_name: v.short_name.into(),
            name: v.name.into(),
            spdx_id: v.spdx_id.map(Into::into),
            website_url: v.website_url.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains information on how externally-hosted data can be ingested into the
/// root dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::SetPollingSource> for SetPollingSource {
    fn from(v: odf::metadata::SetPollingSource) -> Self {
        Self {
            fetch: v.fetch.into(),
            prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
            read: v.read.into(),
            preprocess: v.preprocess.map(Into::into),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a transformation that produces data in a derivative dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetTransform {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
}

impl From<odf::metadata::SetTransform> for SetTransform {
    fn from(v: odf::metadata::SetTransform) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lets you manipulate names of the system columns to avoid conflicts.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
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

impl From<odf::metadata::SetVocab> for SetVocab {
    fn from(v: odf::metadata::SetVocab) -> Self {
        Self {
            offset_column: v.offset_column.map(Into::into),
            operation_type_column: v.operation_type_column.map(Into::into),
            system_time_column: v.system_time_column.map(Into::into),
            event_time_column: v.event_time_column.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines how external data should be cached.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum SourceCaching {
    Forever(SourceCachingForever),
}

impl From<odf::metadata::SourceCaching> for SourceCaching {
    fn from(v: odf::metadata::SourceCaching) -> Self {
        match v {
            odf::metadata::SourceCaching::Forever(v) => Self::Forever(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// After source was processed once it will never be ingested again.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecachingforever-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SourceCachingForever {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::SourceCachingForever> for SourceCachingForever {
    fn from(v: odf::metadata::SourceCachingForever) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies how input files should be ordered before ingestion.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

impl From<odf::metadata::SourceOrdering> for SourceOrdering {
    fn from(v: odf::metadata::SourceOrdering) -> Self {
        match v {
            odf::metadata::SourceOrdering::ByEventTime => Self::ByEventTime,
            odf::metadata::SourceOrdering::ByName => Self::ByName,
        }
    }
}

impl Into<odf::metadata::SourceOrdering> for SourceOrdering {
    fn into(self) -> odf::metadata::SourceOrdering {
        match self {
            Self::ByEventTime => odf::metadata::SourceOrdering::ByEventTime,
            Self::ByName => odf::metadata::SourceOrdering::ByName,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the source the data was added from to allow fast resuming.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SourceState {
    /// Identifies the source that the state corresponds to.
    pub source_name: String,
    /// Identifies the type of the state. Standard types include: `odf/etag`,
    /// `odf/last-modified`.
    pub kind: String,
    /// Opaque value representing the state.
    pub value: String,
}

impl From<odf::metadata::SourceState> for SourceState {
    fn from(v: odf::metadata::SourceState) -> Self {
        Self {
            source_name: v.source_name.into(),
            kind: v.kind.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a query in a multi-step SQL transformation.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SqlQueryStep {
    /// Name of the temporary view that will be created from result of the
    /// query. Step without this alias will be treated as an output of the
    /// transformation.
    pub alias: Option<String>,
    /// SQL query the result of which will be exposed under the alias.
    pub query: String,
}

impl From<odf::metadata::SqlQueryStep> for SqlQueryStep {
    fn from(v: odf::metadata::SqlQueryStep) -> Self {
        Self {
            alias: v.alias.map(Into::into),
            query: v.query.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Temporary Flink-specific extension for creating temporal tables from
/// streams.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TemporalTable {
    /// Name of the dataset to be converted into a temporal table.
    pub name: String,
    /// Column names used as the primary key for creating a table.
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::TemporalTable> for TemporalTable {
    fn from(v: odf::metadata::TemporalTable) -> Self {
        Self {
            name: v.name.into(),
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Engine-specific processing queries that shape the resulting data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    Sql(TransformSql),
}

impl From<odf::metadata::Transform> for Transform {
    fn from(v: odf::metadata::Transform) -> Self {
        match v {
            odf::metadata::Transform::Sql(v) => Self::Sql(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Transform using one of the SQL dialects.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformsql-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformSql {
    pub engine: String,
    pub version: Option<String>,
    pub queries: Vec<SqlQueryStep>,
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

impl From<odf::metadata::TransformSql> for TransformSql {
    fn from(v: odf::metadata::TransformSql) -> Self {
        let queries = if let Some(query) = v.query {
            vec![SqlQueryStep { alias: None, query }]
        } else {
            v.queries.unwrap().into_iter().map(Into::into).collect()
        };

        Self {
            engine: v.engine.into(),
            version: v.version.map(Into::into),
            queries,
            temporal_tables: v
                .temporal_tables
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a derivative transformation input
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum TransformInputDataset {
    Accessible(TransformInputDatasetAccessible),
    NotAccessible(TransformInputDatasetNotAccessible),
}

impl TransformInputDataset {
    pub fn accessible(dataset: Dataset) -> Self {
        Self::Accessible(TransformInputDatasetAccessible { dataset })
    }

    pub fn not_accessible(dataset_ref: odf::DatasetRef) -> Self {
        Self::NotAccessible(TransformInputDatasetNotAccessible {
            dataset_ref: dataset_ref.into(),
        })
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct TransformInputDatasetAccessible {
    pub dataset: Dataset,
}

#[ComplexObject]
impl TransformInputDatasetAccessible {
    async fn message(&self) -> String {
        "Found".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct TransformInputDatasetNotAccessible {
    pub dataset_ref: DatasetRef<'static>,
}

#[ComplexObject]
impl TransformInputDatasetNotAccessible {
    async fn message(&self) -> String {
        "Not Accessible".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
#[graphql(complex)]
pub struct TransformInput {
    pub dataset_ref: DatasetRef<'static>,
    pub alias: String,
}

#[ComplexObject]
impl TransformInput {
    async fn input_dataset(&self, ctx: &Context<'_>) -> Result<TransformInputDataset> {
        Dataset::try_from_ref(ctx, &self.dataset_ref).await
    }
}

impl From<odf::metadata::TransformInput> for TransformInput {
    fn from(v: odf::metadata::TransformInput) -> Self {
        Self {
            dataset_ref: v.dataset_ref.into(),
            alias: v.alias.unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform the next step of data
/// transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformRequest {
    /// Unique identifier of the output dataset.
    pub dataset_id: DatasetID<'static>,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias<'static>,
    /// System time to use for new records.
    pub system_time: DateTime<Utc>,
    /// Vocabulary of the output dataset.
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
    pub prev_checkpoint_path: Option<OSPath>,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_checkpoint_path: OSPath,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_data_path: OSPath,
}

impl From<odf::metadata::TransformRequest> for TransformRequest {
    fn from(v: odf::metadata::TransformRequest) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            dataset_alias: v.dataset_alias.into(),
            system_time: v.system_time.into(),
            vocab: v.vocab.into(),
            transform: v.transform.into(),
            query_inputs: v.query_inputs.into_iter().map(Into::into).collect(),
            next_offset: v.next_offset.into(),
            prev_checkpoint_path: v.prev_checkpoint_path.map(Into::into),
            new_checkpoint_path: v.new_checkpoint_path.into(),
            new_data_path: v.new_data_path.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent as part of the engine transform request operation to describe the input
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformRequestInput {
    /// Unique identifier of the dataset.
    pub dataset_id: DatasetID<'static>,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias<'static>,
    /// An alias of this input to be used in queries.
    pub query_alias: String,
    /// Vocabulary of the input dataset.
    pub vocab: DatasetVocabulary,
    /// Subset of data that goes into this transaction.
    pub offset_interval: Option<OffsetInterval>,
    /// TODO: This will be removed when coordinator will be slicing data for the
    /// engine.
    pub data_paths: Vec<OSPath>,
    /// TODO: replace with actual DDL or Parquet schema.
    pub schema_file: OSPath,
    /// Watermarks that should be injected into the stream to separate micro
    /// batches for reproducibility.
    pub explicit_watermarks: Vec<Watermark>,
}

impl From<odf::metadata::TransformRequestInput> for TransformRequestInput {
    fn from(v: odf::metadata::TransformRequestInput) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            dataset_alias: v.dataset_alias.into(),
            query_alias: v.query_alias.into(),
            vocab: v.vocab.into(),
            offset_interval: v.offset_interval.map(Into::into),
            data_paths: v.data_paths.into_iter().map(Into::into).collect(),
            schema_file: v.schema_file.into(),
            explicit_watermarks: v.explicit_watermarks.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by an engine to coordinator when performing the data transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum TransformResponse {
    Progress(TransformResponseProgress),
    Success(TransformResponseSuccess),
    InvalidQuery(TransformResponseInvalidQuery),
    InternalError(TransformResponseInternalError),
}

impl From<odf::metadata::TransformResponse> for TransformResponse {
    fn from(v: odf::metadata::TransformResponse) -> Self {
        match v {
            odf::metadata::TransformResponse::Progress(v) => Self::Progress(v.into()),
            odf::metadata::TransformResponse::Success(v) => Self::Success(v.into()),
            odf::metadata::TransformResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            odf::metadata::TransformResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinternalerror-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl From<odf::metadata::TransformResponseInternalError> for TransformResponseInternalError {
    fn from(v: odf::metadata::TransformResponseInternalError) -> Self {
        Self {
            message: v.message.into(),
            backtrace: v.backtrace.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinvalidquery-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl From<odf::metadata::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
    fn from(v: odf::metadata::TransformResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseprogress-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::TransformResponseProgress> for TransformResponseProgress {
    fn from(v: odf::metadata::TransformResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponsesuccess-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseSuccess {
    /// Data slice produced by the transaction, if any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any.
    pub new_watermark: Option<DateTime<Utc>>,
}

impl From<odf::metadata::TransformResponseSuccess> for TransformResponseSuccess {
    fn from(v: odf::metadata::TransformResponseSuccess) -> Self {
        Self {
            new_offset_interval: v.new_offset_interval.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a watermark in the event stream.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    /// Moment in processing time when watermark was emitted.
    pub system_time: DateTime<Utc>,
    /// Moment in event time which watermark has reached.
    pub event_time: DateTime<Utc>,
}

impl From<odf::metadata::Watermark> for Watermark {
    fn from(v: odf::metadata::Watermark) -> Self {
        Self {
            system_time: v.system_time.into(),
            event_time: v.event_time.into(),
        }
    }
}
