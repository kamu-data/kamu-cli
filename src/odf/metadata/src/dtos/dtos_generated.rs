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

use bitflags::bitflags;
use chrono::{DateTime, Utc};
use enum_variants::*;

use crate::formats::Multihash;
use crate::identity::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that data has been ingested into a root dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
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

/// Describes how to ingest data into a root dataset from a certain logical
/// source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
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

/// Embedded attachment item.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentEmbedded {
    /// Path to an attachment if it was materialized into a file.
    pub path: String,
    /// Content of the attachment.
    pub content: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the source of attachment files.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

impl_enum_with_variants!(Attachments);
impl_enum_variant!(Attachments::Embedded(AttachmentsEmbedded));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// For attachments that are specified inline and are embedded in the metadata.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentsembedded-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AttachmentsEmbedded {
    /// List of embedded items.
    pub items: Vec<AttachmentEmbedded>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a checkpoint produced by an engine
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Checkpoint {
    /// Hash sum of the checkpoint file.
    pub physical_hash: Multihash,
    /// Size of checkpoint file in bytes.
    pub size: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a compression algorithm.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionFormat {
    Gzip,
    Zip,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// TODO
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datafield-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataField {
    /// TODO
    pub name: String,
    /// Logical type of the field that defines its semantic behavior and value
    /// ranges
    pub r#type: DataType,
    /// ODF extensions
    pub extra: Option<ExtraAttributes>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This schema is aims to be a human-friendly variant of Arrow. Arrow currently specifies only the [flatbuffer format](https://github.com/apache/arrow/blob/f9301c0ba8a7ed1b0b63275cfdd4c44c26b04675/format/Schema.fbs) which has many legacy to it and is not suited to be defined by humans, so we had to define our own schema format. While inspired by Arrow - this format makes a clear separation between logical data types and encoding (physical layout) of data in the chunks.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataschema-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataSchema {
    /// Top-level fields (columns) of the schema.
    pub fields: Vec<DataField>,
    /// ODF extensions
    pub extra: Option<ExtraAttributes>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of data added to a dataset or produced via transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
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

/// Defines a logical type of the field. Logical type determines the semantics
/// and boudaries of a type and how it can be operated on, without a concern
/// about encoding and physical layout of the data in chunks.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatype-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DataType {
    Binary(DataTypeBinary),
    Bool(DataTypeBool),
    Date(DataTypeDate),
    Decimal(DataTypeDecimal),
    Duration(DataTypeDuration),
    Float16(DataTypeFloat16),
    Float32(DataTypeFloat32),
    Float64(DataTypeFloat64),
    Int8(DataTypeInt8),
    Int16(DataTypeInt16),
    Int32(DataTypeInt32),
    Int64(DataTypeInt64),
    UInt8(DataTypeUInt8),
    UInt16(DataTypeUInt16),
    UInt32(DataTypeUInt32),
    UInt64(DataTypeUInt64),
    List(DataTypeList),
    Map(DataTypeMap),
    Null(DataTypeNull),
    Option(DataTypeOption),
    Struct(DataTypeStruct),
    Time(DataTypeTime),
    Timestamp(DataTypeTimestamp),
    String(DataTypeString),
}

impl_enum_with_variants!(DataType);
impl_enum_variant!(DataType::Binary(DataTypeBinary));
impl_enum_variant!(DataType::Bool(DataTypeBool));
impl_enum_variant!(DataType::Date(DataTypeDate));
impl_enum_variant!(DataType::Decimal(DataTypeDecimal));
impl_enum_variant!(DataType::Duration(DataTypeDuration));
impl_enum_variant!(DataType::Float16(DataTypeFloat16));
impl_enum_variant!(DataType::Float32(DataTypeFloat32));
impl_enum_variant!(DataType::Float64(DataTypeFloat64));
impl_enum_variant!(DataType::Int8(DataTypeInt8));
impl_enum_variant!(DataType::Int16(DataTypeInt16));
impl_enum_variant!(DataType::Int32(DataTypeInt32));
impl_enum_variant!(DataType::Int64(DataTypeInt64));
impl_enum_variant!(DataType::UInt8(DataTypeUInt8));
impl_enum_variant!(DataType::UInt16(DataTypeUInt16));
impl_enum_variant!(DataType::UInt32(DataTypeUInt32));
impl_enum_variant!(DataType::UInt64(DataTypeUInt64));
impl_enum_variant!(DataType::List(DataTypeList));
impl_enum_variant!(DataType::Map(DataTypeMap));
impl_enum_variant!(DataType::Null(DataTypeNull));
impl_enum_variant!(DataType::Option(DataTypeOption));
impl_enum_variant!(DataType::Struct(DataTypeStruct));
impl_enum_variant!(DataType::Time(DataTypeTime));
impl_enum_variant!(DataType::Timestamp(DataTypeTimestamp));
impl_enum_variant!(DataType::String(DataTypeString));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A sequence of bytes. Used for arbitrary binary data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypebinary-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeBinary {
    /// Number of bytes per value for fixed-size binary. If omitted, the binary
    /// is variable-length.
    pub fixed_length: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A boolean value representing true or false.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypebool-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeBool {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A calendar date.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypedate-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeDate {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A fixed-point decimal number with a specified precision and scale.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypedecimal-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeDecimal {
    /// Total number of decimal digits that can be stored.
    pub precision: u32,
    /// Number of digits after the decimal point. In certain situations, scale
    /// could be negative number. For negative scale, it is the number of
    /// padding 0 to the right of the digits.
    ///
    /// For example the number 12300 could be treated as a decimal has precision
    /// 3 and scale -2.
    pub scale: i32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An elapsed time interval with a specified time unit.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeduration-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeDuration {
    /// The unit of the duration measurement.
    pub unit: TimeUnit,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A floating-point number.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat16-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeFloat16 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A floating-point number.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat32-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeFloat32 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A floating-point number.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypefloat64-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeFloat64 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint16-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeInt16 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint32-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeInt32 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint64-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeInt64 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeint8-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeInt8 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A list of values, all having the same data type.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypelist-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeList {
    /// Data type of list items.
    pub item_type: Box<DataType>,
    /// Number of list items per value for fixed-size lists. If omitted, the
    /// list is variable-length.
    pub fixed_length: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A map of key-value pairs, represented as a list of entries (structs with key
/// and value fields).
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypemap-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeMap {
    /// Data type of the map's keys.
    pub key_type: Box<DataType>,
    /// Data type of the map's values.
    pub value_type: Box<DataType>,
    /// Set to true if the keys within each value are sorted.
    pub keys_sorted: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A type representing the absence of a value (null).
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypenull-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeNull {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A type representing an optional (nullable) value of another data type.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeoption-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeOption {
    /// Inner data type for the optional value.
    pub inner: Box<DataType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A Unicode string.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypestring-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeString {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A collection of named fields, each with its own data type.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypestruct-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeStruct {
    /// Fields that make up the struct.
    pub fields: Vec<DataField>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A time of day value, without a date, with a specified unit of granularity.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypetime-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeTime {
    /// The unit of the time value.
    pub unit: TimeUnit,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A point in time, represented as an offset from the Unix epoch, with optional
/// timezone.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypetimestamp-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeTimestamp {
    /// The unit of the timestamp value that determines its precision.
    pub unit: TimeUnit,
    /// The timezone is an optional string indicating the name of a timezone
    /// one of
    ///
    /// * As used in the Olson timezone database (the "tz database" or
    ///   "tzdata"), such as "America/New_York".
    /// * An absolute timezone offset of the form "+XX:XX" or "-XX:XX", such as
    ///   "+07:30".
    ///
    /// Whether a timezone string is present indicates different semantics about
    /// the data (see above).
    pub timezone: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint16-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeUInt16 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint32-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeUInt32 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint64-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeUInt64 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An integer value.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datatypeuint8-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataTypeUInt8 {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents type of the dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum DatasetKind {
    Root,
    Derivative,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a projection of the dataset metadata at a single point in time.
/// This type is typically used for defining new datasets and changing the
/// existing ones.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
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

/// Specifies the mapping of system columns onto dataset schema.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
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

/// Disables the previously defined polling source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DisablePollingSource {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DisablePushSource {
    /// Identifies the source to be disabled.
    pub source_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines an environment variable passed into some job.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EnvVar {
    /// Name of the variable.
    pub name: String,
    /// Value of the variable.
    pub value: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum EventTimeSource {
    FromMetadata(EventTimeSourceFromMetadata),
    FromPath(EventTimeSourceFromPath),
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl_enum_with_variants!(EventTimeSource);
impl_enum_variant!(EventTimeSource::FromMetadata(EventTimeSourceFromMetadata));
impl_enum_variant!(EventTimeSource::FromPath(EventTimeSourceFromPath));
impl_enum_variant!(EventTimeSource::FromSystemTime(
    EventTimeSourceFromSystemTime
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the source's metadata.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrommetadata-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromMetadata {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the path component of the source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefrompath-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromPath {
    /// Regular expression where first group contains the timestamp string.
    pub pattern: String,
    /// Format of the expected timestamp in java.text.SimpleDateFormat form.
    pub timestamp_format: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assigns event time from the system time source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesourcefromsystemtime-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromSystemTime {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that derivative transformation has been performed.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
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

/// Describes a slice of the input dataset used during a transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
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

/// Container for custom key-value extension attributes. Every key must be in
/// the form of `<domain>/<path>` (e.g. `kamu.dev/archetype`) in order to fully
/// disambiguate the value in the face of multiple extensions. Values may be any
/// valid JSON including nested objects.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#extraattributes-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExtraAttributes {
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
    Mqtt(FetchStepMqtt),
    EthereumLogs(FetchStepEthereumLogs),
}

impl_enum_with_variants!(FetchStep);
impl_enum_variant!(FetchStep::Url(FetchStepUrl));
impl_enum_variant!(FetchStep::FilesGlob(FetchStepFilesGlob));
impl_enum_variant!(FetchStep::Container(FetchStepContainer));
impl_enum_variant!(FetchStep::Mqtt(FetchStepMqtt));
impl_enum_variant!(FetchStep::EthereumLogs(FetchStepEthereumLogs));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the specified OCI container to fetch data from an arbitrary source.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepcontainer-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Connects to an Ethereum node to stream transaction logs.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepethereumlogs-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uses glob operator to match files on the local file system.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepfilesglob-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Connects to an MQTT broker to fetch events from the specified topic.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepmqtt-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstepurl-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Merge strategy determines how newly ingested data should be combined with
/// the data that already exists in the dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
    ChangelogStream(MergeStrategyChangelogStream),
    UpsertStream(MergeStrategyUpsertStream),
}

impl_enum_with_variants!(MergeStrategy);
impl_enum_variant!(MergeStrategy::Append(MergeStrategyAppend));
impl_enum_variant!(MergeStrategy::Ledger(MergeStrategyLedger));
impl_enum_variant!(MergeStrategy::Snapshot(MergeStrategySnapshot));
impl_enum_variant!(MergeStrategy::ChangelogStream(MergeStrategyChangelogStream));
impl_enum_variant!(MergeStrategy::UpsertStream(MergeStrategyUpsertStream));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Append merge strategy.
///
/// Under this strategy new data will be appended to the dataset in its
/// entirety, without any deduplication.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategyappend-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyAppend {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Changelog stream merge strategy.
///
/// This is the native stream format for ODF that accurately describes the
/// evolution of all event records including appends, retractions, and
/// corrections as per RFC-015. No pre-processing except for format validation
/// is done.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategychangelogstream-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyChangelogStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
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
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyLedger {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
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
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategySnapshot {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime.
    pub primary_key: Vec<String>,
    /// Names of the columns to compared to determine if a row has changed
    /// between two snapshots.
    pub compare_columns: Option<Vec<String>>,
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
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyUpsertStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An individual block in the metadata chain that captures the history of
/// modifications of a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
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

/// Represents a transaction that occurred on a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
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

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MetadataEventTypeFlags: u32 {
        const ADD_DATA = 1 << 0;
        const EXECUTE_TRANSFORM = 1 << 1;
        const SEED = 1 << 2;
        const SET_POLLING_SOURCE = 1 << 3;
        const SET_TRANSFORM = 1 << 4;
        const SET_VOCAB = 1 << 5;
        const SET_ATTACHMENTS = 1 << 6;
        const SET_INFO = 1 << 7;
        const SET_LICENSE = 1 << 8;
        const SET_DATA_SCHEMA = 1 << 9;
        const ADD_PUSH_SOURCE = 1 << 10;
        const DISABLE_PUSH_SOURCE = 1 << 11;
        const DISABLE_POLLING_SOURCE = 1 << 12;
    }
}

impl From<&MetadataEvent> for MetadataEventTypeFlags {
    fn from(v: &MetadataEvent) -> Self {
        match v {
            MetadataEvent::AddData(_) => Self::ADD_DATA,
            MetadataEvent::ExecuteTransform(_) => Self::EXECUTE_TRANSFORM,
            MetadataEvent::Seed(_) => Self::SEED,
            MetadataEvent::SetPollingSource(_) => Self::SET_POLLING_SOURCE,
            MetadataEvent::SetTransform(_) => Self::SET_TRANSFORM,
            MetadataEvent::SetVocab(_) => Self::SET_VOCAB,
            MetadataEvent::SetAttachments(_) => Self::SET_ATTACHMENTS,
            MetadataEvent::SetInfo(_) => Self::SET_INFO,
            MetadataEvent::SetLicense(_) => Self::SET_LICENSE,
            MetadataEvent::SetDataSchema(_) => Self::SET_DATA_SCHEMA,
            MetadataEvent::AddPushSource(_) => Self::ADD_PUSH_SOURCE,
            MetadataEvent::DisablePushSource(_) => Self::DISABLE_PUSH_SOURCE,
            MetadataEvent::DisablePollingSource(_) => Self::DISABLE_POLLING_SOURCE,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT quality of service class.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT topic subscription parameters.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MqttTopicSubscription {
    /// Name of the topic (may include patterns).
    pub path: String,
    /// Quality of service class.
    ///
    /// Defaults to: "AtMostOnce"
    pub qos: Option<MqttQos>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a range of data as a closed arithmetic interval of offsets
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OffsetInterval {
    /// Start of the closed interval [start; end].
    pub start: u64,
    /// End of the closed interval [start; end].
    pub end: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the steps to prepare raw data for ingestion.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

impl_enum_with_variants!(PrepStep);
impl_enum_variant!(PrepStep::Decompress(PrepStepDecompress));
impl_enum_variant!(PrepStep::Pipe(PrepStepPipe));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstepdecompress-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepDecompress {
    /// Name of a compression algorithm used on data.
    pub format: CompressionFormat,
    /// Path to a data file within a multi-file archive. Can contain glob
    /// patterns.
    pub sub_path: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Executes external command to process the data using piped input/output.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepsteppipe-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepPipe {
    /// Command to execute and its arguments.
    pub command: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform query on raw input data,
/// usually as part of ingest preprocessing step
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
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

/// Sent by an engine to coordinator when performing the raw query operation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RawQueryResponse {
    Progress(RawQueryResponseProgress),
    Success(RawQueryResponseSuccess),
    InvalidQuery(RawQueryResponseInvalidQuery),
    InternalError(RawQueryResponseInternalError),
}

impl_enum_with_variants!(RawQueryResponse);
impl_enum_variant!(RawQueryResponse::Progress(RawQueryResponseProgress));
impl_enum_variant!(RawQueryResponse::Success(RawQueryResponseSuccess));
impl_enum_variant!(RawQueryResponse::InvalidQuery(RawQueryResponseInvalidQuery));
impl_enum_variant!(RawQueryResponse::InternalError(
    RawQueryResponseInternalError
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinternalerror-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseinvalidquery-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponseprogress-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseProgress {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponsesuccess-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawQueryResponseSuccess {
    /// Number of records produced by the query
    pub num_records: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines how raw data should be read into the structured form.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
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
impl_enum_variant!(ReadStep::Csv(ReadStepCsv));
impl_enum_variant!(ReadStep::GeoJson(ReadStepGeoJson));
impl_enum_variant!(ReadStep::EsriShapefile(ReadStepEsriShapefile));
impl_enum_variant!(ReadStep::Parquet(ReadStepParquet));
impl_enum_variant!(ReadStep::Json(ReadStepJson));
impl_enum_variant!(ReadStep::NdJson(ReadStepNdJson));
impl_enum_variant!(ReadStep::NdGeoJson(ReadStepNdGeoJson));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for comma-separated files.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepcsv-schema
#[derive(Clone, PartialEq, Eq, Debug)]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for ESRI Shapefile format.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepesrishapefile-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for GeoJSON files. It expects one `FeatureCollection` object in the
/// root and will create a record per each `Feature` inside it extracting the
/// properties into individual columns and leaving the feature geometry in its
/// own column.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepgeojson-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for JSON files that contain an array of objects within them.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepjson-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson`
/// format but instead of `FeatureCollection` object in the root it expects
/// every individual feature object to appear on its own line.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndgeojson-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepNdGeoJson {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for files containing multiple newline-delimited JSON objects with the
/// same schema.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepndjson-schema
#[derive(Clone, PartialEq, Eq, Debug)]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Apache Parquet format.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstepparquet-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepParquet {
    /// A DDL-formatted schema. Schema can be used to coerce values into more
    /// appropriate data types.
    pub schema: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a header (e.g. HTTP) to be passed into some request.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RequestHeader {
    /// Name of the header.
    pub name: String,
    /// Value of the header.
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Establishes the identity of the dataset. Always the first metadata event in
/// the chain.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seed {
    /// Unique identity of the dataset.
    pub dataset_id: DatasetID,
    /// Type of the dataset.
    pub dataset_kind: DatasetKind,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Associates a set of files with this dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetAttachments {
    /// One of the supported attachment sources.
    pub attachments: Attachments,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the complete schema of Data Slices added to the Dataset following
/// this event.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetDataSchema {
    /// DEPRECATED: Apache Arrow schema encoded in its native flatbuffers
    /// representation.
    pub raw_arrow_schema: Option<Vec<u8>>,
    /// Defines the logical schema of the data files that follow this event.
    /// Will become a required field after migration.
    pub schema: Option<DataSchema>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides basic human-readable information about a dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetInfo {
    /// Brief single-sentence summary of a dataset.
    pub description: Option<String>,
    /// Keywords, search terms, or tags used to describe the dataset.
    pub keywords: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a license that applies to this dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
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

/// Contains information on how externally-hosted data can be ingested into the
/// root dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
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

/// Defines a transformation that produces data in a derivative dataset.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetTransform {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lets you manipulate names of the system columns to avoid conflicts.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
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

/// Defines how external data should be cached.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SourceCaching {
    Forever(SourceCachingForever),
}

impl_enum_with_variants!(SourceCaching);
impl_enum_variant!(SourceCaching::Forever(SourceCachingForever));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// After source was processed once it will never be ingested again.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecachingforever-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SourceCachingForever {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies how input files should be ordered before ingestion.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the source the data was added from to allow fast resuming.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
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

/// Defines a query in a multi-step SQL transformation.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
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

/// Temporary Flink-specific extension for creating temporal tables from
/// streams.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TemporalTable {
    /// Name of the dataset to be converted into a temporal table.
    pub name: String,
    /// Column names used as the primary key for creating a table.
    pub primary_key: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// TODO
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#timeunit-schema
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Engine-specific processing queries that shape the resulting data.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Transform {
    Sql(TransformSql),
}

impl_enum_with_variants!(Transform);
impl_enum_variant!(Transform::Sql(TransformSql));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Transform using one of the SQL dialects.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformsql-schema
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a derivative transformation input
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
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

/// Sent by the coordinator to an engine to perform the next step of data
/// transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformRequest {
    /// Unique identifier of the output dataset.
    pub dataset_id: DatasetID,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias,
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
    pub prev_checkpoint_path: Option<PathBuf>,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_checkpoint_path: PathBuf,
    /// TODO: This will be removed when coordinator will be speaking to engines
    /// purely through Arrow.
    pub new_data_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent as part of the engine transform request operation to describe the input
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformRequestInput {
    /// Unique identifier of the dataset.
    pub dataset_id: DatasetID,
    /// Alias of the output dataset, for logging purposes only.
    pub dataset_alias: DatasetAlias,
    /// An alias of this input to be used in queries.
    pub query_alias: String,
    /// Vocabulary of the input dataset.
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

/// Sent by an engine to coordinator when performing the data transformation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TransformResponse {
    Progress(TransformResponseProgress),
    Success(TransformResponseSuccess),
    InvalidQuery(TransformResponseInvalidQuery),
    InternalError(TransformResponseInternalError),
}

impl_enum_with_variants!(TransformResponse);
impl_enum_variant!(TransformResponse::Progress(TransformResponseProgress));
impl_enum_variant!(TransformResponse::Success(TransformResponseSuccess));
impl_enum_variant!(TransformResponse::InvalidQuery(
    TransformResponseInvalidQuery
));
impl_enum_variant!(TransformResponse::InternalError(
    TransformResponseInternalError
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinternalerror-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseinvalidquery-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponseprogress-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseProgress {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponsesuccess-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformResponseSuccess {
    /// Data slice produced by the transaction, if any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any.
    pub new_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a watermark in the event stream.
///
/// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Watermark {
    /// Moment in processing time when watermark was emitted.
    pub system_time: DateTime<Utc>,
    /// Moment in event time which watermark has reached.
    pub event_time: DateTime<Utc>,
}
