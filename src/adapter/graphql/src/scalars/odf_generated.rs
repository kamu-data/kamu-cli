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
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AddData {
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<DataSlice>,
    pub new_checkpoint: Option<Checkpoint>,
    pub new_watermark: Option<DateTime<Utc>>,
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
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AddPushSource {
    pub source_name: String,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
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
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentEmbedded {
    pub path: String,
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
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentsEmbedded {
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
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub physical_hash: Multihash,
    pub size: u64,
}

impl From<odf::Checkpoint> for Checkpoint {
    fn from(v: odf::Checkpoint) -> Self {
        Self {
            physical_hash: v.physical_hash.into(),
            size: v.size.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DataSlice {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    pub offset_interval: OffsetInterval,
    pub size: u64,
}

impl From<odf::DataSlice> for DataSlice {
    fn from(v: odf::DataSlice) -> Self {
        Self {
            logical_hash: v.logical_hash.into(),
            physical_hash: v.physical_hash.into(),
            offset_interval: v.offset_interval.into(),
            size: v.size.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshot {
    pub name: DatasetAlias,
    pub kind: DatasetKind,
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
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetVocabulary {
    pub offset_column: String,
    pub operation_type_column: String,
    pub system_time_column: String,
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
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DisablePushSource {
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
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
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
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromMetadata {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
    fn from(v: odf::metadata::EventTimeSourceFromMetadata) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromPath {
    pub pattern: String,
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
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTransform {
    pub query_inputs: Vec<ExecuteTransformInput>,
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<DataSlice>,
    pub new_checkpoint: Option<Checkpoint>,
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
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteTransformInput {
    pub dataset_id: DatasetID,
    pub prev_block_hash: Option<Multihash>,
    pub new_block_hash: Option<Multihash>,
    pub prev_offset: Option<u64>,
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
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepUrl {
    pub url: String,
    pub event_time: Option<EventTimeSource>,
    pub cache: Option<SourceCaching>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepFilesGlob {
    pub path: String,
    pub event_time: Option<EventTimeSource>,
    pub cache: Option<SourceCaching>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepContainer {
    pub image: String,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepMqtt {
    pub host: String,
    pub port: i32,
    pub username: Option<String>,
    pub password: Option<String>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct FetchStepEthereumLogs {
    pub chain_id: Option<u64>,
    pub node_url: Option<String>,
    pub filter: Option<String>,
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
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
}

impl From<odf::metadata::MergeStrategy> for MergeStrategy {
    fn from(v: odf::metadata::MergeStrategy) -> Self {
        match v {
            odf::metadata::MergeStrategy::Append(v) => Self::Append(v.into()),
            odf::metadata::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            odf::metadata::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyAppend {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::MergeStrategyAppend> for MergeStrategyAppend {
    fn from(v: odf::metadata::MergeStrategyAppend) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyLedger {
    pub primary_key: Vec<String>,
}

impl From<odf::metadata::MergeStrategyLedger> for MergeStrategyLedger {
    fn from(v: odf::metadata::MergeStrategyLedger) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategySnapshot {
    pub primary_key: Vec<String>,
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
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlock {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub sequence_number: u64,
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
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MqttTopicSubscription {
    pub path: String,
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
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct OffsetInterval {
    pub start: u64,
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
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct PrepStepDecompress {
    pub format: CompressionFormat,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct PrepStepPipe {
    pub command: Vec<String>,
}

impl From<odf::metadata::PrepStepPipe> for PrepStepPipe {
    fn from(v: odf::metadata::PrepStepPipe) -> Self {
        Self {
            command: v.command.into_iter().map(Into::into).collect(),
        }
    }
}

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
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryRequest {
    pub input_data_paths: Vec<OSPath>,
    pub transform: Transform,
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
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::RawQueryResponseProgress> for RawQueryResponseProgress {
    fn from(v: odf::metadata::RawQueryResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseSuccess {
    pub num_records: u64,
}

impl From<odf::metadata::RawQueryResponseSuccess> for RawQueryResponseSuccess {
    fn from(v: odf::metadata::RawQueryResponseSuccess) -> Self {
        Self {
            num_records: v.num_records.into(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseInvalidQuery {
    pub message: String,
}

impl From<odf::metadata::RawQueryResponseInvalidQuery> for RawQueryResponseInvalidQuery {
    fn from(v: odf::metadata::RawQueryResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RawQueryResponseInternalError {
    pub message: String,
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
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepCsv {
    pub schema: Option<Vec<String>>,
    pub separator: Option<String>,
    pub encoding: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub header: Option<bool>,
    pub infer_schema: Option<bool>,
    pub null_value: Option<String>,
    pub date_format: Option<String>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepGeoJson {
    pub schema: Option<Vec<String>>,
}

impl From<odf::metadata::ReadStepGeoJson> for ReadStepGeoJson {
    fn from(v: odf::metadata::ReadStepGeoJson) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepEsriShapefile {
    pub schema: Option<Vec<String>>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepParquet {
    pub schema: Option<Vec<String>>,
}

impl From<odf::metadata::ReadStepParquet> for ReadStepParquet {
    fn from(v: odf::metadata::ReadStepParquet) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepJson {
    pub sub_path: Option<String>,
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepNdJson {
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ReadStepNdGeoJson {
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
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub name: String,
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
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Seed {
    pub dataset_id: DatasetID,
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
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetAttachments {
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
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetInfo {
    pub description: Option<String>,
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
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetLicense {
    pub short_name: String,
    pub name: String,
    pub spdx_id: Option<String>,
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
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetPollingSource {
    pub fetch: FetchStep,
    pub prepare: Option<Vec<PrepStep>>,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
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
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetTransform {
    pub inputs: Vec<TransformInput>,
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
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetVocab {
    pub offset_column: Option<String>,
    pub operation_type_column: Option<String>,
    pub system_time_column: Option<String>,
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
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SourceState {
    pub source_name: String,
    pub kind: String,
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
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SqlQueryStep {
    pub alias: Option<String>,
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
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TemporalTable {
    pub name: String,
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
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
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

#[derive(SimpleObject, Debug, Clone)]
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

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct TransformInputDatasetNotAccessible {
    pub dataset_ref: DatasetRef,
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
    pub dataset_ref: DatasetRef,
    pub alias: String,
}

#[ComplexObject]
impl TransformInput {
    async fn dataset(&self, ctx: &Context<'_>) -> Result<TransformInputDataset> {
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
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformRequest {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub system_time: DateTime<Utc>,
    pub vocab: DatasetVocabulary,
    pub transform: Transform,
    pub query_inputs: Vec<TransformRequestInput>,
    pub next_offset: u64,
    pub prev_checkpoint_path: Option<OSPath>,
    pub new_checkpoint_path: OSPath,
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
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformRequestInput {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub query_alias: String,
    pub vocab: DatasetVocabulary,
    pub offset_interval: Option<OffsetInterval>,
    pub data_paths: Vec<OSPath>,
    pub schema_file: OSPath,
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
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::metadata::TransformResponseProgress> for TransformResponseProgress {
    fn from(v: odf::metadata::TransformResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseSuccess {
    pub new_offset_interval: Option<OffsetInterval>,
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

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseInvalidQuery {
    pub message: String,
}

impl From<odf::metadata::TransformResponseInvalidQuery> for TransformResponseInvalidQuery {
    fn from(v: odf::metadata::TransformResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TransformResponseInternalError {
    pub message: String,
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
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub system_time: DateTime<Utc>,
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
