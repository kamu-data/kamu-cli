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
#![allow(dead_code)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

use std::sync::Arc;

use chrono::{DateTime, Utc};
use setty::types::{ByteSize, DurationString};

use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Predefined account specification.
///
/// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct AccountSpec {
    /// DID associated with the account by ODF or an external system
    pub did: Option<AccountID<'static>>,
    /// Type of the account.
    ///
    /// Defaults to: "User"
    pub account_type: Option<AccountType>,
    /// Human-friendly display name.
    pub display_name: Option<String>,
    /// Email address of the account.
    pub email: String,
    /// URL of the account's avatar image.
    pub avatar_url: Option<String>,
    /// Password for local authentication. Absent for SSO or DID-based accounts.
    pub password: Option<Secret>,
}

impl From<odf::metadata::auth::AccountSpec> for AccountSpec {
    fn from(v: odf::metadata::auth::AccountSpec) -> Self {
        Self {
            did: v.did.map(Into::into),
            account_type: v.account_type.map(Into::into),
            display_name: v.display_name.map(Into::into),
            email: v.email.into(),
            avatar_url: v.avatar_url.map(Into::into),
            password: v.password.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the type of an account.
///
/// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/AccountType
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::auth::AccountType")]
pub enum AccountType {
    User,
    Organization,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that data has been ingested into a root dataset.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AddData
#[derive(SimpleObject, Debug, Clone)]
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
    /// ODF extensions.
    pub extra: Option<ExtraAttributes>,
}

impl From<odf::metadata::dataset::AddData> for AddData {
    fn from(v: odf::metadata::dataset::AddData) -> Self {
        Self {
            prev_checkpoint: v.prev_checkpoint.map(Into::into),
            prev_offset: v.prev_offset.map(Into::into),
            new_data: v.new_data.map(Into::into),
            new_checkpoint: v.new_checkpoint.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
            new_source_state: v.new_source_state.map(Into::into),
            extra: v.extra.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes how to ingest data into a root dataset from a certain logical
/// source.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/AddPushSource
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::AddPushSource> for AddPushSource {
    fn from(v: odf::metadata::legacy::AddPushSource) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/AttachmentEmbedded
#[derive(SimpleObject, Debug, Clone)]
pub struct AttachmentEmbedded {
    /// Path to an attachment if it was materialized into a file.
    pub path: String,
    /// Content of the attachment.
    pub content: String,
}

impl From<odf::metadata::dataset::AttachmentEmbedded> for AttachmentEmbedded {
    fn from(v: odf::metadata::dataset::AttachmentEmbedded) -> Self {
        Self {
            path: v.path.into(),
            content: v.content.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the source of attachment files.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments
#[derive(Union, Debug, Clone)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

impl From<odf::metadata::dataset::Attachments> for Attachments {
    fn from(v: odf::metadata::dataset::Attachments) -> Self {
        match v {
            odf::metadata::dataset::Attachments::Embedded(v) => Self::Embedded(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// For attachments that are specified inline and are embedded in the metadata.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Attachments#/$defs/Embedded
#[derive(SimpleObject, Debug, Clone)]
pub struct AttachmentsEmbedded {
    /// List of embedded items.
    pub items: Vec<AttachmentEmbedded>,
}

impl From<odf::metadata::dataset::AttachmentsEmbedded> for AttachmentsEmbedded {
    fn from(v: odf::metadata::dataset::AttachmentsEmbedded) -> Self {
        Self {
            items: v.items.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A named attribute attached to a resource, used by auth policies for access
/// control decisions.
///
/// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Attribute
#[derive(SimpleObject, Debug, Clone)]
pub struct Attribute {
    /// The resource this attribute is attached to.
    pub object: ResourceRef,
    /// Name of the attribute e.g. `allowPublicRead`.
    pub name: String,
    /// Value of the attribute.
    pub value: serde_json::Value,
}

impl From<odf::metadata::auth::Attribute> for Attribute {
    fn from(v: odf::metadata::auth::Attribute) -> Self {
        Self {
            object: v.object.into(),
            name: v.name.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Access credentials for AWS or an AWS-compatible service.
///
/// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/AwsCredentials
#[derive(SimpleObject, Debug, Clone)]
pub struct AwsCredentials {
    /// Reference to a secret containing the AWS access key ID.
    pub access_key: Option<ValueRef>,
    /// Reference to a secret containing the AWS secret access key.
    pub secret_key: Option<ValueRef>,
}

impl From<odf::metadata::storage::AwsCredentials> for AwsCredentials {
    fn from(v: odf::metadata::storage::AwsCredentials) -> Self {
        Self {
            access_key: v.access_key.map(Into::into),
            secret_key: v.secret_key.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a checkpoint produced by an engine
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Checkpoint
#[derive(SimpleObject, Debug, Clone)]
pub struct Checkpoint {
    /// Hash sum of the checkpoint file.
    pub physical_hash: Multihash<'static>,
    /// Size of checkpoint file in bytes.
    pub size: u64,
}

impl From<odf::metadata::dataset::Checkpoint> for Checkpoint {
    fn from(v: odf::metadata::dataset::Checkpoint) -> Self {
        Self {
            physical_hash: v.physical_hash.into(),
            size: v.size.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Optional parameters to control ingestion behavior.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/CompactionParams
#[derive(SimpleObject, Debug, Clone)]
pub struct CompactionParams {
    /// Target maximum size of each compacted data slice e.g. `100MiB`.
    pub max_slice_size: Option<ByteSize>,
    /// Target maximum number of records per compacted data slice.
    pub max_slice_records: Option<u64>,
}

impl From<odf::metadata::dataset::CompactionParams> for CompactionParams {
    fn from(v: odf::metadata::dataset::CompactionParams) -> Self {
        Self {
            max_slice_size: v.max_slice_size.map(Into::into),
            max_slice_records: v.max_slice_records.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a compression algorithm.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/CompressionFormat
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::source::CompressionFormat")]
pub enum CompressionFormat {
    Gzip,
    Zip,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a slice of data added to a dataset or produced via transformation
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DataSlice
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::DataSlice> for DataSlice {
    fn from(v: odf::metadata::dataset::DataSlice) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetKind
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::dataset::DatasetKind")]
pub enum DatasetKind {
    Root,
    Derivative,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a projection of the dataset metadata at a single point in time.
/// This type is typically used for defining new datasets and changing the
/// existing ones.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/DatasetSnapshot
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::DatasetSnapshot> for DatasetSnapshot {
    fn from(v: odf::metadata::legacy::DatasetSnapshot) -> Self {
        Self {
            name: v.name.into(),
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a desired state of the dataset metadata.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct DatasetSpec {
    /// DID of the dataset in global ODF network
    pub did: Option<DatasetID<'static>>,
    /// Type of the dataset.
    pub kind: DatasetKind,
    /// An array of metadata events that will be used to populate the chain.
    /// Here you can define polling and push sources, set licenses, add
    /// attachments etc.
    pub metadata: Vec<MetadataEvent>,
    /// Reference to a storage volume where dataset data will be stored. If
    /// omitted, the node's default storage is used.
    pub volume: Option<PersistentVolumeRef>,
}

impl From<odf::metadata::dataset::DatasetSpec> for DatasetSpec {
    fn from(v: odf::metadata::dataset::DatasetSpec) -> Self {
        Self {
            did: v.did.map(Into::into),
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
            volume: v.volume.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the mapping of system columns onto dataset schema.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/DatasetVocabulary
#[derive(SimpleObject, Debug, Clone)]
pub struct DatasetVocabulary {
    /// Name of the offset column.
    ///
    /// Defaults to: "offset"
    pub offset_column: Option<String>,
    /// Name of the operation type column.
    ///
    /// Defaults to: "op"
    pub operation_type_column: Option<String>,
    /// Name of the system time column.
    ///
    /// Defaults to: "system_time"
    pub system_time_column: Option<String>,
    /// Name of the event time column.
    ///
    /// Defaults to: "event_time"
    pub event_time_column: Option<String>,
}

impl From<odf::metadata::dataset::DatasetVocabulary> for DatasetVocabulary {
    fn from(v: odf::metadata::dataset::DatasetVocabulary) -> Self {
        Self {
            offset_column: v.offset_column.map(Into::into),
            operation_type_column: v.operation_type_column.map(Into::into),
            system_time_column: v.system_time_column.map(Into::into),
            event_time_column: v.event_time_column.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined polling source.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePollingSource
#[derive(SimpleObject, Debug, Clone)]
pub struct DisablePollingSource {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::legacy::DisablePollingSource> for DisablePollingSource {
    fn from(v: odf::metadata::legacy::DisablePollingSource) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Disables the previously defined source.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/DisablePushSource
#[derive(SimpleObject, Debug, Clone)]
pub struct DisablePushSource {
    /// Identifies the source to be disabled.
    pub source_name: String,
}

impl From<odf::metadata::legacy::DisablePushSource> for DisablePushSource {
    fn from(v: odf::metadata::legacy::DisablePushSource) -> Self {
        Self {
            source_name: v.source_name.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines an environment variable passed into some job.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EnvVar
#[derive(SimpleObject, Debug, Clone)]
pub struct EnvVar {
    /// Name of the variable.
    pub name: String,
    /// Value of the variable.
    pub value: Option<String>,
}

impl From<odf::metadata::source::EnvVar> for EnvVar {
    fn from(v: odf::metadata::source::EnvVar) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Filters that work on domain event types and fields.
///
/// Schema: https://opendatafabric.org/schemas/event/v1alpha1/EventFilter

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct EventFilter(odf::metadata::event::EventFilter);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for EventFilter {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::event::EventFilter =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::event::EventFilter = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource
#[derive(Union, Debug, Clone)]
pub enum EventTimeSource {
    FromMetadata(EventTimeSourceFromMetadata),
    FromPath(EventTimeSourceFromPath),
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl From<odf::metadata::source::EventTimeSource> for EventTimeSource {
    fn from(v: odf::metadata::source::EventTimeSource) -> Self {
        match v {
            odf::metadata::source::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
            odf::metadata::source::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
            odf::metadata::source::EventTimeSource::FromSystemTime(v) => {
                Self::FromSystemTime(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the source's metadata.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromMetadata
#[derive(SimpleObject, Debug, Clone)]
pub struct EventTimeSourceFromMetadata {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::source::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
    fn from(v: odf::metadata::source::EventTimeSourceFromMetadata) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extracts event time from the path component of the source.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromPath
#[derive(SimpleObject, Debug, Clone)]
pub struct EventTimeSourceFromPath {
    /// Regular expression where first group contains the timestamp string.
    pub pattern: String,
    /// Format of the expected timestamp in java.text.SimpleDateFormat form.
    pub timestamp_format: Option<String>,
}

impl From<odf::metadata::source::EventTimeSourceFromPath> for EventTimeSourceFromPath {
    fn from(v: odf::metadata::source::EventTimeSourceFromPath) -> Self {
        Self {
            pattern: v.pattern.into(),
            timestamp_format: v.timestamp_format.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assigns event time from the system time source.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/EventTimeSource#/$defs/FromSystemTime
#[derive(SimpleObject, Debug, Clone)]
pub struct EventTimeSourceFromSystemTime {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::source::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
    fn from(v: odf::metadata::source::EventTimeSourceFromSystemTime) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indicates that derivative transformation has been performed.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransform
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::ExecuteTransform> for ExecuteTransform {
    fn from(v: odf::metadata::dataset::ExecuteTransform) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ExecuteTransformInput
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::ExecuteTransformInput> for ExecuteTransformInput {
    fn from(v: odf::metadata::dataset::ExecuteTransformInput) -> Self {
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

/// Container for custom key-value extension attributes. Every key must be in
/// the form of `<domain>/<path>` (e.g. `kamu.dev/archetype`) in order to fully
/// disambiguate the value in the face of multiple extensions. Values may be any
/// valid JSON including nested objects.
///
/// Schema: https://opendatafabric.org/schemas/data/v1alpha1/ExtraAttributes

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct ExtraAttributes(odf::metadata::data::ExtraAttributes);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ExtraAttributes {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::data::ExtraAttributes =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::data::ExtraAttributes = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the external source of data.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep
#[derive(Union, Debug, Clone)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
    Mqtt(FetchStepMqtt),
    EthereumLogs(FetchStepEthereumLogs),
}

impl From<odf::metadata::legacy::FetchStep> for FetchStep {
    fn from(v: odf::metadata::legacy::FetchStep) -> Self {
        match v {
            odf::metadata::legacy::FetchStep::Url(v) => Self::Url(v.into()),
            odf::metadata::legacy::FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
            odf::metadata::legacy::FetchStep::Container(v) => Self::Container(v.into()),
            odf::metadata::legacy::FetchStep::Mqtt(v) => Self::Mqtt(v.into()),
            odf::metadata::legacy::FetchStep::EthereumLogs(v) => Self::EthereumLogs(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the specified OCI container to fetch data from an arbitrary source.
///
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Container
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::FetchStepContainer> for FetchStepContainer {
    fn from(v: odf::metadata::legacy::FetchStepContainer) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/EthereumLogs
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::FetchStepEthereumLogs> for FetchStepEthereumLogs {
    fn from(v: odf::metadata::legacy::FetchStepEthereumLogs) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/FilesGlob
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::FetchStepFilesGlob> for FetchStepFilesGlob {
    fn from(v: odf::metadata::legacy::FetchStepFilesGlob) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Mqtt
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::FetchStepMqtt> for FetchStepMqtt {
    fn from(v: odf::metadata::legacy::FetchStepMqtt) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/legacy/v0/FetchStep#/$defs/Url
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::FetchStepUrl> for FetchStepUrl {
    fn from(v: odf::metadata::legacy::FetchStepUrl) -> Self {
        Self {
            url: v.url.into(),
            event_time: v.event_time.map(Into::into),
            cache: v.cache.map(Into::into),
            headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a sequence of tasks to be executed upon certain trigger conditions.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct FlowSpec {
    /// Defines resources for which this flow will be instantiated.
    pub target: ResourceSelector,
    /// Conditions that cause this flow to execute.
    pub triggers: Vec<FlowTrigger>,
    /// List of tasks to run consecutively.
    pub tasks: Vec<TaskSpec>,
}

impl From<odf::metadata::flow::FlowSpec> for FlowSpec {
    fn from(v: odf::metadata::flow::FlowSpec) -> Self {
        Self {
            target: v.target.into(),
            triggers: v.triggers.into_iter().map(Into::into).collect(),
            tasks: v.tasks.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Condition that causes a flow to be executed.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger
#[derive(Union, Debug, Clone)]
pub enum FlowTrigger {
    Schedule(FlowTriggerSchedule),
    Event(FlowTriggerEvent),
    Source(FlowTriggerSource),
    Dataset(FlowTriggerDataset),
}

impl From<odf::metadata::flow::FlowTrigger> for FlowTrigger {
    fn from(v: odf::metadata::flow::FlowTrigger) -> Self {
        match v {
            odf::metadata::flow::FlowTrigger::Schedule(v) => Self::Schedule(v.into()),
            odf::metadata::flow::FlowTrigger::Event(v) => Self::Event(v.into()),
            odf::metadata::flow::FlowTrigger::Source(v) => Self::Source(v.into()),
            odf::metadata::flow::FlowTrigger::Dataset(v) => Self::Dataset(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Triggers the flow when matching datasets are updated.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Dataset
#[derive(SimpleObject, Debug, Clone)]
pub struct FlowTriggerDataset {
    /// Selector that identifies which datasets can trigger this flow.
    pub dataset: DatasetSelector,
    /// Set of event bus event IDs that this trigger will react to
    pub events: Option<Vec<String>>,
}

impl From<odf::metadata::flow::FlowTriggerDataset> for FlowTriggerDataset {
    fn from(v: odf::metadata::flow::FlowTriggerDataset) -> Self {
        Self {
            dataset: v.dataset.into(),
            events: v.events.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Triggers the flow when an event bus event matching one of the filters is
/// observed.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Event
#[derive(SimpleObject, Debug, Clone)]
pub struct FlowTriggerEvent {
    /// Filters the event by type and fields.
    pub events: EventFilter,
    /// The trigger will fire upon first observed event. If another event
    /// arrives withing the `cooldown` interval the firing will be postponed
    /// until `cooldown` interval ends. I.e. trigger is guaranteed to fire, but
    /// may batch multiple events together into one flow run.
    pub cooldown: Option<DurationString>,
    /// If an event is observed a `cooldownMaxBatch` number of times during the
    /// `cooldown` interval it will fire the trigger without waiting for
    /// cooldown to finish.
    pub cooldown_max_batch: Option<u64>,
}

impl From<odf::metadata::flow::FlowTriggerEvent> for FlowTriggerEvent {
    fn from(v: odf::metadata::flow::FlowTriggerEvent) -> Self {
        Self {
            events: v.events.into(),
            cooldown: v.cooldown.map(Into::into),
            cooldown_max_batch: v.cooldown_max_batch.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Triggers the flow on a cron schedule.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Schedule
#[derive(SimpleObject, Debug, Clone)]
pub struct FlowTriggerSchedule {
    /// Cron5 expression defining the schedule e.g. `@daily` or `*/30 * * * *`.
    pub cron: String,
}

impl From<odf::metadata::flow::FlowTriggerSchedule> for FlowTriggerSchedule {
    fn from(v: odf::metadata::flow::FlowTriggerSchedule) -> Self {
        Self {
            cron: v.cron.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Triggers the flow when a source receives new data, with optional batching
/// controls.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/FlowTrigger#/$defs/Source
#[derive(SimpleObject, Debug, Clone)]
pub struct FlowTriggerSource {
    /// Reference to the source resource that drives this trigger.
    pub source: ResourceRef,
    /// Minimum number of new records to accumulate before triggering.
    pub min_records_to_await: Option<u64>,
    /// Maximum time to wait for `minRecordsToAwait` before triggering anyway
    /// e.g. `1h`.
    pub max_await_interval: Option<DurationString>,
}

impl From<odf::metadata::flow::FlowTriggerSource> for FlowTriggerSource {
    fn from(v: odf::metadata::flow::FlowTriggerSource) -> Self {
        Self {
            source: v.source.into(),
            min_records_to_await: v.min_records_to_await.map(Into::into),
            max_await_interval: v.max_await_interval.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Optional parameters to control ingestion behavior.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngestParams
#[derive(SimpleObject, Debug, Clone)]
pub struct IngestParams {
    /// Target number of records to ingest per data slice.
    pub target_slice_records: Option<u64>,
}

impl From<odf::metadata::source::IngestParams> for IngestParams {
    fn from(v: odf::metadata::source::IngestParams) -> Self {
        Self {
            target_slice_records: v.target_slice_records.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the point where data enters the system.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress
#[derive(Union, Debug, Clone)]
pub enum Ingress {
    Url(IngressUrl),
    FilesGlob(IngressFilesGlob),
    Container(IngressContainer),
    Mqtt(IngressMqtt),
    EvmLogs(IngressEvmLogs),
    RestEndpoint(IngressRestEndpoint),
}

impl From<odf::metadata::source::Ingress> for Ingress {
    fn from(v: odf::metadata::source::Ingress) -> Self {
        match v {
            odf::metadata::source::Ingress::Url(v) => Self::Url(v.into()),
            odf::metadata::source::Ingress::FilesGlob(v) => Self::FilesGlob(v.into()),
            odf::metadata::source::Ingress::Container(v) => Self::Container(v.into()),
            odf::metadata::source::Ingress::Mqtt(v) => Self::Mqtt(v.into()),
            odf::metadata::source::Ingress::EvmLogs(v) => Self::EvmLogs(v.into()),
            odf::metadata::source::Ingress::RestEndpoint(v) => Self::RestEndpoint(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the specified OCI container to fetch data from an arbitrary source.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Container
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressContainer {
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

impl From<odf::metadata::source::IngressContainer> for IngressContainer {
    fn from(v: odf::metadata::source::IngressContainer) -> Self {
        Self {
            image: v.image.into(),
            command: v.command.map(|v| v.into_iter().map(Into::into).collect()),
            args: v.args.map(|v| v.into_iter().map(Into::into).collect()),
            env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Connects to an EVM (Ethereum) node to stream transaction logs.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/EvmLogs
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressEvmLogs {
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

impl From<odf::metadata::source::IngressEvmLogs> for IngressEvmLogs {
    fn from(v: odf::metadata::source::IngressEvmLogs) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/FilesGlob
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressFilesGlob {
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

impl From<odf::metadata::source::IngressFilesGlob> for IngressFilesGlob {
    fn from(v: odf::metadata::source::IngressFilesGlob) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Mqtt
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressMqtt {
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

impl From<odf::metadata::source::IngressMqtt> for IngressMqtt {
    fn from(v: odf::metadata::source::IngressMqtt) -> Self {
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

/// Exposes a REST HTTP endpoint that accepts pushed data records.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/RestEndpoint
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressRestEndpoint {
    /// Buffer configuration for holding records until they are ingested.
    pub buffer: Option<IngressBuffer>,
}

impl From<odf::metadata::source::IngressRestEndpoint> for IngressRestEndpoint {
    fn from(v: odf::metadata::source::IngressRestEndpoint) -> Self {
        Self {
            buffer: v.buffer.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/Ingress#/$defs/Url
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressUrl {
    /// URL of the data source
    pub url: String,
    /// Describes how event time is extracted from the source metadata.
    pub event_time: Option<EventTimeSource>,
    /// Describes the caching settings used for this source.
    pub cache: Option<SourceCaching>,
    /// Headers to pass during the request (e.g. HTTP Authorization)
    pub headers: Option<Vec<RequestHeader>>,
}

impl From<odf::metadata::source::IngressUrl> for IngressUrl {
    fn from(v: odf::metadata::source::IngressUrl) -> Self {
        Self {
            url: v.url.into(),
            event_time: v.event_time.map(Into::into),
            cache: v.cache.map(Into::into),
            headers: v.headers.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Buffer configuration for holding pushed records until they are ingested.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer
#[derive(Union, Debug, Clone)]
pub enum IngressBuffer {
    Memory(IngressBufferMemory),
}

impl From<odf::metadata::source::IngressBuffer> for IngressBuffer {
    fn from(v: odf::metadata::source::IngressBuffer) -> Self {
        match v {
            odf::metadata::source::IngressBuffer::Memory(v) => Self::Memory(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An in-memory buffer.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/IngressBuffer#/$defs/Memory
#[derive(SimpleObject, Debug, Clone)]
pub struct IngressBufferMemory {
    /// Maximum number of records to hold in the buffer.
    pub buffer_size: Option<u64>,
    /// Policy applied when the buffer is full.
    pub overflow_policy: Option<String>,
}

impl From<odf::metadata::source::IngressBufferMemory> for IngressBufferMemory {
    fn from(v: odf::metadata::source::IngressBufferMemory) -> Self {
        Self {
            buffer_size: v.buffer_size.map(Into::into),
            overflow_policy: v.overflow_policy.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Filters that work on resource labels and identity headers.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/LabelFilter

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct LabelFilter(odf::metadata::resource::LabelFilter);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for LabelFilter {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::resource::LabelFilter =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::resource::LabelFilter = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Merge strategy determines how newly ingested data should be combined with
/// the data that already exists in the dataset.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy
#[derive(Union, Debug, Clone)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
    ChangelogStream(MergeStrategyChangelogStream),
    UpsertStream(MergeStrategyUpsertStream),
}

impl From<odf::metadata::source::MergeStrategy> for MergeStrategy {
    fn from(v: odf::metadata::source::MergeStrategy) -> Self {
        match v {
            odf::metadata::source::MergeStrategy::Append(v) => Self::Append(v.into()),
            odf::metadata::source::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            odf::metadata::source::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
            odf::metadata::source::MergeStrategy::ChangelogStream(v) => {
                Self::ChangelogStream(v.into())
            }
            odf::metadata::source::MergeStrategy::UpsertStream(v) => Self::UpsertStream(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Append merge strategy.
///
/// Under this strategy new data will be appended to the dataset in its
/// entirety, without any deduplication.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Append
#[derive(SimpleObject, Debug, Clone)]
pub struct MergeStrategyAppend {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::source::MergeStrategyAppend> for MergeStrategyAppend {
    fn from(v: odf::metadata::source::MergeStrategyAppend) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/ChangelogStream
#[derive(SimpleObject, Debug, Clone)]
pub struct MergeStrategyChangelogStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::source::MergeStrategyChangelogStream> for MergeStrategyChangelogStream {
    fn from(v: odf::metadata::source::MergeStrategyChangelogStream) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Ledger
#[derive(SimpleObject, Debug, Clone)]
pub struct MergeStrategyLedger {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::source::MergeStrategyLedger> for MergeStrategyLedger {
    fn from(v: odf::metadata::source::MergeStrategyLedger) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/Snapshot
#[derive(SimpleObject, Debug, Clone)]
pub struct MergeStrategySnapshot {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime.
    pub primary_key: Vec<String>,
    /// Names of the columns to compared to determine if a row has changed
    /// between two snapshots.
    pub compare_columns: Option<Vec<String>>,
}

impl From<odf::metadata::source::MergeStrategySnapshot> for MergeStrategySnapshot {
    fn from(v: odf::metadata::source::MergeStrategySnapshot) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MergeStrategy#/$defs/UpsertStream
#[derive(SimpleObject, Debug, Clone)]
pub struct MergeStrategyUpsertStream {
    /// Names of the columns that uniquely identify the record throughout its
    /// lifetime
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::source::MergeStrategyUpsertStream> for MergeStrategyUpsertStream {
    fn from(v: odf::metadata::source::MergeStrategyUpsertStream) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An individual block in the metadata chain that captures the history of
/// modifications of a dataset.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataBlock
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::MetadataBlock> for MetadataBlock {
    fn from(v: odf::metadata::dataset::MetadataBlock) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/MetadataEvent
#[derive(Union, Debug, Clone)]
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

impl From<odf::metadata::dataset::MetadataEvent> for MetadataEvent {
    fn from(v: odf::metadata::dataset::MetadataEvent) -> Self {
        match v {
            odf::metadata::dataset::MetadataEvent::AddData(v) => Self::AddData(v.into()),
            odf::metadata::dataset::MetadataEvent::ExecuteTransform(v) => {
                Self::ExecuteTransform(v.into())
            }
            odf::metadata::dataset::MetadataEvent::Seed(v) => Self::Seed(v.into()),
            odf::metadata::dataset::MetadataEvent::SetPollingSource(v) => {
                Self::SetPollingSource(v.into())
            }
            odf::metadata::dataset::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
            odf::metadata::dataset::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
            odf::metadata::dataset::MetadataEvent::SetAttachments(v) => {
                Self::SetAttachments(v.into())
            }
            odf::metadata::dataset::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
            odf::metadata::dataset::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
            odf::metadata::dataset::MetadataEvent::SetDataSchema(v) => {
                Self::SetDataSchema(v.into())
            }
            odf::metadata::dataset::MetadataEvent::AddPushSource(v) => {
                Self::AddPushSource(v.into())
            }
            odf::metadata::dataset::MetadataEvent::DisablePushSource(v) => {
                Self::DisablePushSource(v.into())
            }
            odf::metadata::dataset::MetadataEvent::DisablePollingSource(v) => {
                Self::DisablePollingSource(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT quality of service class.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttQos
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::source::MqttQos")]
pub enum MqttQos {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// MQTT topic subscription parameters.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/MqttTopicSubscription
#[derive(SimpleObject, Debug, Clone)]
pub struct MqttTopicSubscription {
    /// Name of the topic (may include patterns).
    pub path: String,
    /// Quality of service class.
    ///
    /// Defaults to: "AtMostOnce"
    pub qos: Option<MqttQos>,
}

impl From<odf::metadata::source::MqttTopicSubscription> for MqttTopicSubscription {
    fn from(v: odf::metadata::source::MqttTopicSubscription) -> Self {
        Self {
            path: v.path.into(),
            qos: v.qos.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a range of data as a closed arithmetic interval of offsets
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/OffsetInterval
#[derive(SimpleObject, Debug, Clone)]
pub struct OffsetInterval {
    /// Start of the closed interval [start; end].
    pub start: u64,
    /// End of the closed interval [start; end].
    pub end: u64,
}

impl From<odf::metadata::dataset::OffsetInterval> for OffsetInterval {
    fn from(v: odf::metadata::dataset::OffsetInterval) -> Self {
        Self {
            start: v.start.into(),
            end: v.end.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a storage volume where data can be stored and its access
/// credentials.
///
/// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec
#[derive(Union, Debug, Clone)]
pub enum PersistentVolumeSpec {
    S3(PersistentVolumeSpecS3),
}

impl From<odf::metadata::storage::PersistentVolumeSpec> for PersistentVolumeSpec {
    fn from(v: odf::metadata::storage::PersistentVolumeSpec) -> Self {
        match v {
            odf::metadata::storage::PersistentVolumeSpec::S3(v) => Self::S3(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An Amazon S3 or S3-compatible object storage bucket.
///
/// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/PersistentVolumeSpec#/$defs/S3
#[derive(SimpleObject, Debug, Clone)]
pub struct PersistentVolumeSpecS3 {
    /// S3 endpoint URL. If omitted, defaults to AWS S3. Use for S3-compatible stores e.g. `https://s3.amazonaws.com`.
    pub endpoint: Option<String>,
    /// AWS region where the bucket is located e.g. `us-west-2`.
    pub region: Option<String>,
    /// Name of the S3 bucket.
    pub bucket: String,
    /// Optional path prefix within the bucket.
    pub prefix: Option<String>,
    /// Storage capacity allocation.
    pub capacity: Option<VolumeCapacity>,
    /// Access credentials for the bucket.
    pub credentials: Option<AwsCredentials>,
}

impl From<odf::metadata::storage::PersistentVolumeSpecS3> for PersistentVolumeSpecS3 {
    fn from(v: odf::metadata::storage::PersistentVolumeSpecS3) -> Self {
        Self {
            endpoint: v.endpoint.map(Into::into),
            region: v.region.map(Into::into),
            bucket: v.bucket.into(),
            prefix: v.prefix.map(Into::into),
            capacity: v.capacity.map(Into::into),
            credentials: v.credentials.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines the steps to prepare raw data for ingestion.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep
#[derive(Union, Debug, Clone)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

impl From<odf::metadata::source::PrepStep> for PrepStep {
    fn from(v: odf::metadata::source::PrepStep) -> Self {
        match v {
            odf::metadata::source::PrepStep::Decompress(v) => Self::Decompress(v.into()),
            odf::metadata::source::PrepStep::Pipe(v) => Self::Pipe(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls data from one of the supported sources by its URL.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Decompress
#[derive(SimpleObject, Debug, Clone)]
pub struct PrepStepDecompress {
    /// Name of a compression algorithm used on data.
    pub format: CompressionFormat,
    /// Path to a data file within a multi-file archive. Can contain glob
    /// patterns.
    pub sub_path: Option<String>,
}

impl From<odf::metadata::source::PrepStepDecompress> for PrepStepDecompress {
    fn from(v: odf::metadata::source::PrepStepDecompress) -> Self {
        Self {
            format: v.format.into(),
            sub_path: v.sub_path.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Executes external command to process the data using piped input/output.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/PrepStep#/$defs/Pipe
#[derive(SimpleObject, Debug, Clone)]
pub struct PrepStepPipe {
    /// Command to execute and its arguments.
    pub command: Vec<String>,
}

impl From<odf::metadata::source::PrepStepPipe> for PrepStepPipe {
    fn from(v: odf::metadata::source::PrepStepPipe) -> Self {
        Self {
            command: v.command.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a projection of a dataaset history into a state for fast lookups.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/ProjectionSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct ProjectionSpec {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub project: Transform,
}

impl From<odf::metadata::dataset::ProjectionSpec> for ProjectionSpec {
    fn from(v: odf::metadata::dataset::ProjectionSpec) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            project: v.project.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sent by the coordinator to an engine to perform query on raw input data,
/// usually as part of ingest preprocessing step
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryRequest
#[derive(SimpleObject, Debug, Clone)]
pub struct RawQueryRequest {
    /// Paths to input data files to perform query over. Must all have identical
    /// schema.
    pub input_data_paths: Vec<OSPath>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
    /// Path where query result will be written.
    pub output_data_path: OSPath,
}

impl From<odf::metadata::engine::RawQueryRequest> for RawQueryRequest {
    fn from(v: odf::metadata::engine::RawQueryRequest) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse
#[derive(Union, Debug, Clone)]
pub enum RawQueryResponse {
    Progress(RawQueryResponseProgress),
    Success(RawQueryResponseSuccess),
    InvalidQuery(RawQueryResponseInvalidQuery),
    InternalError(RawQueryResponseInternalError),
}

impl From<odf::metadata::engine::RawQueryResponse> for RawQueryResponse {
    fn from(v: odf::metadata::engine::RawQueryResponse) -> Self {
        match v {
            odf::metadata::engine::RawQueryResponse::Progress(v) => Self::Progress(v.into()),
            odf::metadata::engine::RawQueryResponse::Success(v) => Self::Success(v.into()),
            odf::metadata::engine::RawQueryResponse::InvalidQuery(v) => {
                Self::InvalidQuery(v.into())
            }
            odf::metadata::engine::RawQueryResponse::InternalError(v) => {
                Self::InternalError(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InternalError
#[derive(SimpleObject, Debug, Clone)]
pub struct RawQueryResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl From<odf::metadata::engine::RawQueryResponseInternalError> for RawQueryResponseInternalError {
    fn from(v: odf::metadata::engine::RawQueryResponseInternalError) -> Self {
        Self {
            message: v.message.into(),
            backtrace: v.backtrace.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/InvalidQuery
#[derive(SimpleObject, Debug, Clone)]
pub struct RawQueryResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl From<odf::metadata::engine::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
    fn from(v: odf::metadata::engine::RawQueryResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Progress
#[derive(SimpleObject, Debug, Clone)]
pub struct RawQueryResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::engine::RawQueryResponseProgress> for RawQueryResponseProgress {
    fn from(v: odf::metadata::engine::RawQueryResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/RawQueryResponse#/$defs/Success
#[derive(SimpleObject, Debug, Clone)]
pub struct RawQueryResponseSuccess {
    /// Number of records produced by the query
    pub num_records: u64,
}

impl From<odf::metadata::engine::RawQueryResponseSuccess> for RawQueryResponseSuccess {
    fn from(v: odf::metadata::engine::RawQueryResponseSuccess) -> Self {
        Self {
            num_records: v.num_records.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines how raw data should be read into the structured form.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep
#[derive(Interface, Debug, Clone)]
#[graphql(field(
    name = "schema",
    ty = "Option<crate::prelude::DataSchema>",
    arg(name = "format", ty = "Option<DataSchemaFormat>"),
))]
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Csv
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepCsv {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    ///
    /// Examples:
    /// - ["date TIMESTAMP","city STRING","population INT"]
    pub ddl_schema: Option<Vec<String>>,
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

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepCsv> for ReadStepCsv {
    fn from(v: odf::metadata::ReadStepCsv) -> Self {
        let odf::metadata::ReadStepCsv {
            ddl_schema,
            separator,
            encoding,
            quote,
            escape,
            header,
            infer_schema,
            null_value,
            date_format,
            timestamp_format,
            schema,
        } = v;

        Self {
            ddl_schema,
            separator,
            encoding,
            quote,
            escape,
            header,
            infer_schema,
            null_value,
            date_format,
            timestamp_format,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepCsv {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for ESRI Shapefile format.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/EsriShapefile
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepEsriShapefile {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,
    /// If the ZIP archive contains multiple shapefiles use this field to
    /// specify a sub-path to the desired `.shp` file. Can contain glob patterns
    /// to act as a filter.
    pub sub_path: Option<String>,

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepEsriShapefile> for ReadStepEsriShapefile {
    fn from(v: odf::metadata::ReadStepEsriShapefile) -> Self {
        let odf::metadata::ReadStepEsriShapefile {
            ddl_schema,
            sub_path,
            schema,
        } = v;
        Self {
            ddl_schema,
            sub_path,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepEsriShapefile {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for GeoJSON files. It expects one `FeatureCollection` object in the
/// root and will create a record per each `Feature` inside it extracting the
/// properties into individual columns and leaving the feature geometry in its
/// own column.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/GeoJson
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepGeoJson {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepGeoJson> for ReadStepGeoJson {
    fn from(v: odf::metadata::ReadStepGeoJson) -> Self {
        let odf::metadata::ReadStepGeoJson { ddl_schema, schema } = v;
        Self {
            ddl_schema,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepGeoJson {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for JSON files that contain an array of objects within them.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Json
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepJson {
    /// Path in the form of `a.b.c` to a sub-element of the root JSON object
    /// that is an array or objects. If not specified it is assumed that the
    /// root element is an array.
    pub sub_path: Option<String>,
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,
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

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepJson> for ReadStepJson {
    fn from(v: odf::metadata::ReadStepJson) -> Self {
        let odf::metadata::ReadStepJson {
            sub_path,
            ddl_schema,
            date_format,
            encoding,
            timestamp_format,
            schema,
        } = v;

        Self {
            sub_path,
            ddl_schema,
            date_format,
            encoding,
            timestamp_format,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepJson {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Newline-delimited GeoJSON files. It is similar to `GeoJson`
/// format but instead of `FeatureCollection` object in the root it expects
/// every individual feature object to appear on its own line.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdGeoJson
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepNdGeoJson {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepNdGeoJson> for ReadStepNdGeoJson {
    fn from(v: odf::metadata::ReadStepNdGeoJson) -> Self {
        let odf::metadata::ReadStepNdGeoJson { ddl_schema, schema } = v;
        Self {
            ddl_schema,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepNdGeoJson {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for files containing multiple newline-delimited JSON objects with the
/// same schema.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/NdJson
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepNdJson {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,
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

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepNdJson> for ReadStepNdJson {
    fn from(v: odf::metadata::ReadStepNdJson) -> Self {
        let odf::metadata::ReadStepNdJson {
            ddl_schema,
            date_format,
            encoding,
            timestamp_format,
            schema,
        } = v;

        Self {
            ddl_schema,
            date_format,
            encoding,
            timestamp_format,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepNdJson {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reader for Apache Parquet format.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/ReadStep#/$defs/Parquet
#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ReadStepParquet {
    /// DEPRECATED: A DDL-formatted schema. Schema can be used to coerce values
    /// into more appropriate data types.
    pub ddl_schema: Option<Vec<String>>,

    #[graphql(skip)]
    pub schema: Option<Arc<odf::schema::DataSchema>>,
}

impl From<odf::metadata::ReadStepParquet> for ReadStepParquet {
    fn from(v: odf::metadata::ReadStepParquet) -> Self {
        let odf::metadata::ReadStepParquet { ddl_schema, schema } = v;
        Self {
            ddl_schema,
            schema: schema.map(Arc::new),
        }
    }
}

#[ComplexObject]
impl ReadStepParquet {
    /// Schema used to coerce values into more appropriate data types.
    async fn schema(&self, format: Option<DataSchemaFormat>) -> Option<crate::prelude::DataSchema> {
        self.schema
            .clone()
            .or_else(|| {
                self.ddl_schema.as_ref().and_then(|s| {
                    odf::utils::schema::parse::parse_ddl_to_odf_schema(&s.join(", "))
                        .ok()
                        .map(Arc::new)
                })
            })
            .map(|s| {
                crate::prelude::DataSchema::new(s, format.unwrap_or(DataSchemaFormat::OdfJson))
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A directed relationship between two resources, optionally carrying a typed
/// value.
///
/// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/Relation
#[derive(SimpleObject, Debug, Clone)]
pub struct Relation {
    /// The resource that holds the relation.
    pub subject: ResourceRef,
    /// Name of the relation e.g. `role`, `member`, `owner`.
    pub relation: String,
    /// Optional value associated with the relation e.g. `maintainer` for a
    /// `role` relation.
    pub value: Option<serde_json::Value>,
    /// The resource that is the target of the relation.
    pub object: ResourceRef,
}

impl From<odf::metadata::auth::Relation> for Relation {
    fn from(v: odf::metadata::auth::Relation) -> Self {
        Self {
            subject: v.subject.into(),
            relation: v.relation.into(),
            value: v.value.map(Into::into),
            object: v.object.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies resource attributes and relations between resources on which auth
/// policies act upon.
///
/// Schema: https://opendatafabric.org/schemas/auth/v1alpha1/RelationsSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct RelationsSpec {
    /// Relations between resources.
    pub relations: Option<Vec<Relation>>,
    /// Resource attributes.
    pub attributes: Option<Vec<Attribute>>,
}

impl From<odf::metadata::auth::RelationsSpec> for RelationsSpec {
    fn from(v: odf::metadata::auth::RelationsSpec) -> Self {
        Self {
            relations: v.relations.map(|v| v.into_iter().map(Into::into).collect()),
            attributes: v
                .attributes
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a header (e.g. HTTP) to be passed into some request.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/RequestHeader
#[derive(SimpleObject, Debug, Clone)]
pub struct RequestHeader {
    /// Name of the header.
    pub name: String,
    /// Value of the header.
    pub value: String,
}

impl From<odf::metadata::source::RequestHeader> for RequestHeader {
    fn from(v: odf::metadata::source::RequestHeader) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Top-level container for resources that specifies the type and version of the
/// resource it's specifying and carries identity, ownership, and status
/// information.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/Resource
#[derive(SimpleObject, Debug, Clone)]
pub struct Resource {
    /// Identifies the controlling entity, a bounded context that this resource belongs to, and the version. Url should follow the pattern `{base-url}/{context}/{version}/{name}.json` e.g. `https://opendatafabric.org/schemas/dataset/v1/Dataset.json`.
    pub schema: TypeUri<'static>,
    /// Container for identity and ownership information of a resource.
    pub headers: ResourceHeaders,
    /// Specifies the desired state of a resource.
    pub spec: serde_json::Value,
    /// Resource lifecycle and reconciliation inforamtion.
    pub status: Option<ResourceStatus>,
}

impl From<odf::metadata::resource::Resource<serde_json::Value>> for Resource {
    fn from(v: odf::metadata::resource::Resource<serde_json::Value>) -> Self {
        Self {
            schema: v.schema.into(),
            headers: v.headers.into(),
            spec: v.spec.into(),
            status: v.status.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Annotations is an unstructured key value map stored with a resource that may
/// be set by external tools to store and retrieve arbitrary metadata. Unlike
/// labels, annotations are not indexed and cannot be queried by.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceAnnotations

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct ResourceAnnotations(odf::metadata::resource::ResourceAnnotations);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ResourceAnnotations {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::resource::ResourceAnnotations =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::resource::ResourceAnnotations =
            self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Container of feneric contditions that can be added by contollers to provide additional information about the state of a resource. Keys uniquely identify the condition and should be in the form of URL to a schema describing this condition, e.g. `https://opendatafabric.org/schemas/resource/ConditionReady.json`.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceConditions

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct ResourceConditions(odf::metadata::resource::ResourceConditions);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ResourceConditions {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::resource::ResourceConditions =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::resource::ResourceConditions =
            self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Container for identity and ownership information of a resource.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceHeaders
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceHeaders {
    /// Unique identifier of a resource within entire ODF node. Automatically
    /// assigned upon resource creation.
    pub id: Option<ResourceID<'static>>,
    /// Symbolic name of a resource that identifies it within a scope of an
    /// onwing account.
    pub name: ResourceName<'static>,
    /// Reference to an account that owns the resource.
    pub account: Option<AccountRef>,
    /// Map of string keys and values that can be used to organize, categorize,
    /// and query resources.
    pub labels: Option<ResourceLabels>,
    /// Annotations is an unstructured key value map stored with a resource that
    /// may be set by external tools to store and retrieve arbitrary metadata.
    /// Unlike labels, annotations are not indexed and cannot be queried by.
    pub annotations: Option<ResourceAnnotations>,
    /// A sequential number that changes every time the resource header and spec
    /// are updated. Does not increment on status changes, thus signifying
    /// changes to the desired state. Populated by the system. Starts with `1`.
    pub generation: Option<u64>,
    /// Time when the resource was first applied and assigned an identity.
    pub created_at: Option<DateTime<Utc>>,
    /// Time when the resource was last updated, including header, spec, and
    /// status updates.
    pub updated_at: Option<DateTime<Utc>>,
    /// Time when the resource was deleted.
    pub deleted_at: Option<DateTime<Utc>>,
}

impl From<odf::metadata::resource::ResourceHeaders> for ResourceHeaders {
    fn from(v: odf::metadata::resource::ResourceHeaders) -> Self {
        Self {
            id: v.id.map(Into::into),
            name: v.name.into(),
            account: v.account.map(Into::into),
            labels: v.labels.map(Into::into),
            annotations: v.annotations.map(Into::into),
            generation: v.generation.map(Into::into),
            created_at: v.created_at.map(Into::into),
            updated_at: v.updated_at.map(Into::into),
            deleted_at: v.deleted_at.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Map of string keys and values that can be used to organize, categorize, and
/// query resources.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceLabels

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct ResourceLabels(odf::metadata::resource::ResourceLabels);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ResourceLabels {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::resource::ResourceLabels =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::resource::ResourceLabels =
            self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the lifecycle stage of a resource.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourcePhase
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::resource::ResourcePhase")]
pub enum ResourcePhase {
    Pending,
    Reconciling,
    Ready,
    Failed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resource lifecycle and reconciliation inforamtion.
///
/// Schema: https://opendatafabric.org/schemas/resource/v1alpha1/ResourceStatus
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceStatus {
    /// Represents the lifecycle stage of a resource.
    pub phase: ResourcePhase,
    /// Resource generation that was last processed by the main resource
    /// controller.
    pub observed_generation: Option<u64>,
    /// Time when the controller last reconciled the desired resource state as
    /// defined in `observedGeneration`.
    pub reconciled_at: Option<DateTime<Utc>>,
    /// Detailed conditions describing the state of the resource that are added
    /// by controllers.
    pub conditions: Option<ResourceConditions>,
}

impl From<odf::metadata::resource::ResourceStatus> for ResourceStatus {
    fn from(v: odf::metadata::resource::ResourceStatus) -> Self {
        Self {
            phase: v.phase.into(),
            observed_generation: v.observed_generation.map(Into::into),
            reconciled_at: v.reconciled_at.map(Into::into),
            conditions: v.conditions.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Individual secret in raw or encrypted form.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secret
#[derive(SimpleObject, Debug, Clone)]
pub struct Secret {
    /// A secret value in raw or encoded form.
    pub value: String,
    /// Represents the encoding of the value. Typically will be `jwe` after a
    /// raw secret gets encrypted.
    pub content_encoding: Option<String>,
}

impl From<odf::metadata::config::Secret> for Secret {
    fn from(v: odf::metadata::config::Secret) -> Self {
        Self {
            value: v.value.into(),
            content_encoding: v.content_encoding.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a set of secrets stored and managed by the ODF node and accessible
/// via embedded sercets provider.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSetSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct SecretSetSpec {
    /// Key value pairs of secrets.
    pub secrets: Secrets,
}

impl From<odf::metadata::config::SecretSetSpec> for SecretSetSpec {
    fn from(v: odf::metadata::config::SecretSetSpec) -> Self {
        Self {
            secrets: v.secrets.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Container for key-value secrets. Every key must be a string. Values may be
/// strings with raw unencrypted data or objects that signify the encoding.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Secrets

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct Secrets(odf::metadata::config::Secrets);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for Secrets {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::config::Secrets = async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::config::Secrets = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Establishes the identity of the dataset. Always the first metadata event in
/// the chain.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Seed
#[derive(SimpleObject, Debug, Clone)]
pub struct Seed {
    /// Unique identity of the dataset.
    pub dataset_id: DatasetID<'static>,
    /// Type of the dataset.
    pub dataset_kind: DatasetKind,
}

impl From<odf::metadata::dataset::Seed> for Seed {
    fn from(v: odf::metadata::dataset::Seed) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            dataset_kind: v.dataset_kind.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Associates a set of files with this dataset.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetAttachments
#[derive(SimpleObject, Debug, Clone)]
pub struct SetAttachments {
    /// One of the supported attachment sources.
    pub attachments: Attachments,
}

impl From<odf::metadata::dataset::SetAttachments> for SetAttachments {
    fn from(v: odf::metadata::dataset::SetAttachments) -> Self {
        Self {
            attachments: v.attachments.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies the complete schema of Data Slices added to the Dataset following
/// this event.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetDataSchema
#[derive(Debug, Clone)]
pub struct SetDataSchema {
    pub schema: std::sync::Arc<odf::schema::DataSchema>,
}

#[Object]
impl SetDataSchema {
    // TODO: Make `format` required argument
    async fn schema(&self, format: Option<DataSchemaFormat>) -> crate::prelude::DataSchema {
        crate::prelude::DataSchema::new(
            self.schema.clone(),
            format.unwrap_or(DataSchemaFormat::OdfJson),
        )
    }
}

impl From<odf::metadata::SetDataSchema> for SetDataSchema {
    fn from(v: odf::metadata::SetDataSchema) -> Self {
        Self {
            schema: std::sync::Arc::new(v.upgrade().schema),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides basic human-readable information about a dataset.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetInfo
#[derive(SimpleObject, Debug, Clone)]
pub struct SetInfo {
    /// Brief single-sentence summary of a dataset.
    pub description: Option<String>,
    /// Keywords, search terms, or tags used to describe the dataset.
    pub keywords: Option<Vec<String>>,
}

impl From<odf::metadata::dataset::SetInfo> for SetInfo {
    fn from(v: odf::metadata::dataset::SetInfo) -> Self {
        Self {
            description: v.description.map(Into::into),
            keywords: v.keywords.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a license that applies to this dataset.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetLicense
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::SetLicense> for SetLicense {
    fn from(v: odf::metadata::dataset::SetLicense) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/legacy/v0/SetPollingSource
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::legacy::SetPollingSource> for SetPollingSource {
    fn from(v: odf::metadata::legacy::SetPollingSource) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetTransform
#[derive(SimpleObject, Debug, Clone)]
pub struct SetTransform {
    /// Datasets that will be used as sources.
    pub inputs: Vec<TransformInput>,
    /// Transformation that will be applied to produce new data.
    pub transform: Transform,
}

impl From<odf::metadata::dataset::SetTransform> for SetTransform {
    fn from(v: odf::metadata::dataset::SetTransform) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lets you manipulate names of the system columns to avoid conflicts.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SetVocab
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::dataset::SetVocab> for SetVocab {
    fn from(v: odf::metadata::dataset::SetVocab) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching
#[derive(Union, Debug, Clone)]
pub enum SourceCaching {
    Forever(SourceCachingForever),
}

impl From<odf::metadata::source::SourceCaching> for SourceCaching {
    fn from(v: odf::metadata::source::SourceCaching) -> Self {
        match v {
            odf::metadata::source::SourceCaching::Forever(v) => Self::Forever(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// After source was processed once it will never be ingested again.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceCaching#/$defs/Forever
#[derive(SimpleObject, Debug, Clone)]
pub struct SourceCachingForever {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::source::SourceCachingForever> for SourceCachingForever {
    fn from(v: odf::metadata::source::SourceCachingForever) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies how input files should be ordered before ingestion.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceOrdering
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::source::SourceOrdering")]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Specifies an external source of data for ingestion.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct SourceSpec {
    /// Brings the configuration values into the local `config` context.
    pub config: Option<ValueRefs>,
    /// Determines where data is sourced from.
    pub ingress: Option<Ingress>,
    /// Defines how raw data is prepared before reading.
    pub prepare: Option<Vec<PrepStep>>,
    /// Defines how data is read into structured format.
    pub read: ReadStep,
    /// Pre-processing query that shapes the data.
    pub preprocess: Option<Transform>,
    /// Determines how newly-ingested data should be merged with existing
    /// history.
    pub merge: Option<MergeStrategy>,
    /// Defines the mapping of system fields to dataset column names.
    pub vocab: Option<DatasetVocabulary>,
}

impl From<odf::metadata::source::SourceSpec> for SourceSpec {
    fn from(v: odf::metadata::source::SourceSpec) -> Self {
        Self {
            config: v.config.map(Into::into),
            ingress: v.ingress.map(Into::into),
            prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
            read: v.read.into(),
            preprocess: v.preprocess.map(Into::into),
            merge: v.merge.map(Into::into),
            vocab: v.vocab.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The state of the source the data was added from to allow fast resuming.
///
/// Schema: https://opendatafabric.org/schemas/source/v1alpha1/SourceState
#[derive(SimpleObject, Debug, Clone)]
pub struct SourceState {
    /// Identifies the source that the state corresponds to.
    pub source_name: String,
    /// Identifies the type of the state. Standard types include: `odf/etag`,
    /// `odf/last-modified`.
    pub kind: String,
    /// Opaque value representing the state.
    pub value: String,
}

impl From<odf::metadata::source::SourceState> for SourceState {
    fn from(v: odf::metadata::source::SourceState) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/SqlQueryStep
#[derive(SimpleObject, Debug, Clone)]
pub struct SqlQueryStep {
    /// Name of the temporary view that will be created from result of the
    /// query. Step without this alias will be treated as an output of the
    /// transformation.
    pub alias: Option<String>,
    /// SQL query the result of which will be exposed under the alias.
    pub query: String,
}

impl From<odf::metadata::dataset::SqlQueryStep> for SqlQueryStep {
    fn from(v: odf::metadata::dataset::SqlQueryStep) -> Self {
        Self {
            alias: v.alias.map(Into::into),
            query: v.query.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An individual work item to be executed as part of a flow.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec
#[derive(Union, Debug, Clone)]
pub enum TaskSpec {
    Ingest(TaskSpecIngest),
    Compaction(TaskSpecCompaction),
    GarbageCollection(TaskSpecGarbageCollection),
    WebhookCall(TaskSpecWebhookCall),
}

impl From<odf::metadata::flow::TaskSpec> for TaskSpec {
    fn from(v: odf::metadata::flow::TaskSpec) -> Self {
        match v {
            odf::metadata::flow::TaskSpec::Ingest(v) => Self::Ingest(v.into()),
            odf::metadata::flow::TaskSpec::Compaction(v) => Self::Compaction(v.into()),
            odf::metadata::flow::TaskSpec::GarbageCollection(v) => {
                Self::GarbageCollection(v.into())
            }
            odf::metadata::flow::TaskSpec::WebhookCall(v) => Self::WebhookCall(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compacts data files in matching datasets to improve query performance.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Compaction
#[derive(SimpleObject, Debug, Clone)]
pub struct TaskSpecCompaction {
    /// Optional parameters to control ingestion behavior.
    pub params: Option<CompactionParams>,
}

impl From<odf::metadata::flow::TaskSpecCompaction> for TaskSpecCompaction {
    fn from(v: odf::metadata::flow::TaskSpecCompaction) -> Self {
        Self {
            params: v.params.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Removes unreferenced data files from matching datasets.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/GarbageCollection
#[derive(SimpleObject, Debug, Clone)]
pub struct TaskSpecGarbageCollection {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::flow::TaskSpecGarbageCollection> for TaskSpecGarbageCollection {
    fn from(v: odf::metadata::flow::TaskSpecGarbageCollection) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fetches data from a source and appends it to a dataset.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/Ingest
#[derive(SimpleObject, Debug, Clone)]
pub struct TaskSpecIngest {
    /// Reference to the source resource that defines how to fetch data.
    pub source: ResourceRef,
    /// Optional parameters to control ingestion behavior.
    pub params: Option<IngestParams>,
}

impl From<odf::metadata::flow::TaskSpecIngest> for TaskSpecIngest {
    fn from(v: odf::metadata::flow::TaskSpecIngest) -> Self {
        Self {
            source: v.source.into(),
            params: v.params.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Dispatches a certain payload to a specific `WebhookTarget`.
///
/// Schema: https://opendatafabric.org/schemas/flow/v1alpha1/TaskSpec#/$defs/WebhookCall
#[derive(SimpleObject, Debug, Clone)]
pub struct TaskSpecWebhookCall {
    /// Reference to the `WebhookTarget`.
    pub target: ResourceRef,
    /// The payload to send. May include templating.
    pub payload: Option<String>,
}

impl From<odf::metadata::flow::TaskSpecWebhookCall> for TaskSpecWebhookCall {
    fn from(v: odf::metadata::flow::TaskSpecWebhookCall) -> Self {
        Self {
            target: v.target.into(),
            payload: v.payload.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Temporary Flink-specific extension for creating temporal tables from
/// streams.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TemporalTable
#[derive(SimpleObject, Debug, Clone)]
pub struct TemporalTable {
    /// Name of the dataset to be converted into a temporal table.
    pub name: String,
    /// Column names used as the primary key for creating a table.
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::dataset::TemporalTable> for TemporalTable {
    fn from(v: odf::metadata::dataset::TemporalTable) -> Self {
        Self {
            name: v.name.into(),
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Engine-specific processing queries that shape the resulting data.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform
#[derive(Union, Debug, Clone)]
pub enum Transform {
    Sql(TransformSql),
}

impl From<odf::metadata::dataset::Transform> for Transform {
    fn from(v: odf::metadata::dataset::Transform) -> Self {
        match v {
            odf::metadata::dataset::Transform::Sql(v) => Self::Sql(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Transform using one of the SQL dialects.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Transform#/$defs/Sql
#[derive(SimpleObject, Debug, Clone)]
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
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/TransformInput
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

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct TransformInput {
    pub dataset_ref: DatasetRef<'static>,
    pub alias: String,
}

#[ComplexObject]
impl TransformInput {
    async fn input_dataset(&self, ctx: &Context<'_>) -> Result<TransformInputDataset> {
        if let Some(dataset) = Dataset::try_from_ref(ctx, &self.dataset_ref).await? {
            Ok(TransformInputDataset::accessible(dataset))
        } else {
            Ok(TransformInputDataset::not_accessible(
                self.dataset_ref.clone().into(),
            ))
        }
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
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequest
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::engine::TransformRequest> for TransformRequest {
    fn from(v: odf::metadata::engine::TransformRequest) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformRequestInput
#[derive(SimpleObject, Debug, Clone)]
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

impl From<odf::metadata::engine::TransformRequestInput> for TransformRequestInput {
    fn from(v: odf::metadata::engine::TransformRequestInput) -> Self {
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
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse
#[derive(Union, Debug, Clone)]
pub enum TransformResponse {
    Progress(TransformResponseProgress),
    Success(TransformResponseSuccess),
    InvalidQuery(TransformResponseInvalidQuery),
    InternalError(TransformResponseInternalError),
}

impl From<odf::metadata::engine::TransformResponse> for TransformResponse {
    fn from(v: odf::metadata::engine::TransformResponse) -> Self {
        match v {
            odf::metadata::engine::TransformResponse::Progress(v) => Self::Progress(v.into()),
            odf::metadata::engine::TransformResponse::Success(v) => Self::Success(v.into()),
            odf::metadata::engine::TransformResponse::InvalidQuery(v) => {
                Self::InvalidQuery(v.into())
            }
            odf::metadata::engine::TransformResponse::InternalError(v) => {
                Self::InternalError(v.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Internal error during query execution
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InternalError
#[derive(SimpleObject, Debug, Clone)]
pub struct TransformResponseInternalError {
    /// Brief description of an error
    pub message: String,
    /// Details of an error (e.g. a backtrace)
    pub backtrace: Option<String>,
}

impl From<odf::metadata::engine::TransformResponseInternalError>
    for TransformResponseInternalError
{
    fn from(v: odf::metadata::engine::TransformResponseInternalError) -> Self {
        Self {
            message: v.message.into(),
            backtrace: v.backtrace.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query did not pass validation
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/InvalidQuery
#[derive(SimpleObject, Debug, Clone)]
pub struct TransformResponseInvalidQuery {
    /// Explanation of an error
    pub message: String,
}

impl From<odf::metadata::engine::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
    fn from(v: odf::metadata::engine::TransformResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reports query progress
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Progress
#[derive(SimpleObject, Debug, Clone)]
pub struct TransformResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::engine::TransformResponseProgress> for TransformResponseProgress {
    fn from(v: odf::metadata::engine::TransformResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
///
/// Schema: https://opendatafabric.org/schemas/engine/v1alpha1/TransformResponse#/$defs/Success
#[derive(SimpleObject, Debug, Clone)]
pub struct TransformResponseSuccess {
    /// Data slice produced by the transaction, if any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any.
    pub new_watermark: Option<DateTime<Utc>>,
}

impl From<odf::metadata::engine::TransformResponseSuccess> for TransformResponseSuccess {
    fn from(v: odf::metadata::engine::TransformResponseSuccess) -> Self {
        Self {
            new_offset_interval: v.new_offset_interval.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Container for key-value variables. Every key must be a string. Values shoud
/// reference fields in `SecretSet`s and `VariableSet`s.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/ValueRefs

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct ValueRefs(odf::metadata::config::ValueRefs);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for ValueRefs {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::config::ValueRefs =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::config::ValueRefs = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Individual variable.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variable
#[derive(SimpleObject, Debug, Clone)]
pub struct Variable {
    /// A value in raw or encoded form.
    pub value: String,
}

impl From<odf::metadata::config::Variable> for Variable {
    fn from(v: odf::metadata::config::Variable) -> Self {
        Self {
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a set of variables stored and managed by the ODF node and accessible
/// via embedded variables provider.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSetSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct VariableSetSpec {
    /// Key value pairs of variables.
    pub variables: Variables,
}

impl From<odf::metadata::config::VariableSetSpec> for VariableSetSpec {
    fn from(v: odf::metadata::config::VariableSetSpec) -> Self {
        Self {
            variables: v.variables.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Container for key-value variables. Every key must be a string. Values may be
/// raw strings or objects that incorporate the encoding.
///
/// Schema: https://opendatafabric.org/schemas/config/v1alpha1/Variables

#[nutype::nutype(derive(AsRef, Clone, Debug, From, Into))]
pub struct Variables(odf::metadata::config::Variables);

#[async_graphql::Scalar]
impl async_graphql::ScalarType for Variables {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let proxy: odf::metadata::serde::yaml::config::Variables =
            async_graphql::from_value(value)?;
        let dto = proxy
            .try_into()
            .map_err(|e: odf::metadata::errors::ValidationError| {
                async_graphql::InputValueError::custom(e.to_string())
            })?;
        Ok(Self::new(dto))
    }

    fn to_value(&self) -> async_graphql::Value {
        let value: odf::metadata::serde::yaml::config::Variables = self.as_ref().clone().into();
        async_graphql::to_value(&value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Storage capacity allocation.
///
/// Schema: https://opendatafabric.org/schemas/storage/v1alpha1/VolumeCapacity
#[derive(SimpleObject, Debug, Clone)]
pub struct VolumeCapacity {
    /// Maximum storage size e.g. `10Gi`.
    pub storage: Option<ByteSize>,
}

impl From<odf::metadata::storage::VolumeCapacity> for VolumeCapacity {
    fn from(v: odf::metadata::storage::VolumeCapacity) -> Self {
        Self {
            storage: v.storage.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a watermark in the event stream.
///
/// Schema: https://opendatafabric.org/schemas/dataset/v1alpha1/Watermark
#[derive(SimpleObject, Debug, Clone)]
pub struct Watermark {
    /// Moment in processing time when watermark was emitted.
    pub system_time: DateTime<Utc>,
    /// Moment in event time which watermark has reached.
    pub event_time: DateTime<Utc>,
}

impl From<odf::metadata::dataset::Watermark> for Watermark {
    fn from(v: odf::metadata::dataset::Watermark) -> Self {
        Self {
            system_time: v.system_time.into(),
            event_time: v.event_time.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a webhook target endpoint that can receive event notifications and
/// data.
///
/// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetSpec
#[derive(SimpleObject, Debug, Clone)]
pub struct WebhookTargetSpec {
    /// Target url of the webhook.
    pub url: String,
    /// Shared secret used for HMAC signature of the request payload for
    /// authentication.
    pub secret: Option<Secret>,
}

impl From<odf::metadata::sink::WebhookTargetSpec> for WebhookTargetSpec {
    fn from(v: odf::metadata::sink::WebhookTargetSpec) -> Self {
        Self {
            url: v.url.into(),
            secret: v.secret.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the status of the webhook target endpoint.
///
/// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus
#[derive(SimpleObject, Debug, Clone)]
pub struct WebhookTargetStatus {
    /// Status value.
    pub value: WebhookTargetStatusValue,
}

impl From<odf::metadata::sink::WebhookTargetStatus> for WebhookTargetStatus {
    fn from(v: odf::metadata::sink::WebhookTargetStatus) -> Self {
        Self {
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Status of the target endpoint
///
/// Schema: https://opendatafabric.org/schemas/sink/v1alpha1/WebhookTargetStatus#/$defs/Value
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::metadata::sink::WebhookTargetStatusValue")]
pub enum WebhookTargetStatusValue {
    Ready,
    Failed,
}
