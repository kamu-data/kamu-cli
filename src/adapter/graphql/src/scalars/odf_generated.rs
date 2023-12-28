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

#![allow(unused_variables)]

use chrono::{DateTime, Utc};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AddData {
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<DataSlice>,
    pub new_checkpoint: Option<Checkpoint>,
    pub new_watermark: Option<DateTime<Utc>>,
    pub new_source_state: Option<SourceState>,
}

impl From<odf::AddData> for AddData {
    fn from(v: odf::AddData) -> Self {
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

////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AddPushSource {
    pub source_name: Option<String>,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

impl From<odf::AddPushSource> for AddPushSource {
    fn from(v: odf::AddPushSource) -> Self {
        Self {
            source_name: v.source_name.map(Into::into),
            read: v.read.into(),
            preprocess: v.preprocess.map(Into::into),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentEmbedded {
    pub path: String,
    pub content: String,
}

impl From<odf::AttachmentEmbedded> for AttachmentEmbedded {
    fn from(v: odf::AttachmentEmbedded) -> Self {
        Self {
            path: v.path.into(),
            content: v.content.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum Attachments {
    Embedded(AttachmentsEmbedded),
}

impl From<odf::Attachments> for Attachments {
    fn from(v: odf::Attachments) -> Self {
        match v {
            odf::Attachments::Embedded(v) => Self::Embedded(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct AttachmentsEmbedded {
    pub items: Vec<AttachmentEmbedded>,
}

impl From<odf::AttachmentsEmbedded> for AttachmentsEmbedded {
    fn from(v: odf::AttachmentsEmbedded) -> Self {
        Self {
            items: v.items.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetKind {
    Root,
    Derivative,
}

impl From<odf::DatasetKind> for DatasetKind {
    fn from(v: odf::DatasetKind) -> Self {
        match v {
            odf::DatasetKind::Root => Self::Root,
            odf::DatasetKind::Derivative => Self::Derivative,
        }
    }
}

impl Into<odf::DatasetKind> for DatasetKind {
    fn into(self) -> odf::DatasetKind {
        match self {
            Self::Root => odf::DatasetKind::Root,
            Self::Derivative => odf::DatasetKind::Derivative,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetSnapshot {
    pub name: DatasetAlias,
    pub kind: DatasetKind,
    pub metadata: Vec<MetadataEvent>,
}

impl From<odf::DatasetSnapshot> for DatasetSnapshot {
    fn from(v: odf::DatasetSnapshot) -> Self {
        Self {
            name: v.name.into(),
            kind: v.kind.into(),
            metadata: v.metadata.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetVocabulary {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
}

impl From<odf::DatasetVocabulary> for DatasetVocabulary {
    fn from(v: odf::DatasetVocabulary) -> Self {
        Self {
            system_time_column: v.system_time_column.map(Into::into),
            event_time_column: v.event_time_column.map(Into::into),
            offset_column: v.offset_column.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DisablePollingSource {
    pub _dummy: Option<String>,
}

impl From<odf::DisablePollingSource> for DisablePollingSource {
    fn from(v: odf::DisablePollingSource) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DisablePushSource {
    pub source_name: Option<String>,
}

impl From<odf::DisablePushSource> for DisablePushSource {
    fn from(v: odf::DisablePushSource) -> Self {
        Self {
            source_name: v.source_name.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
    pub value: Option<String>,
}

impl From<odf::EnvVar> for EnvVar {
    fn from(v: odf::EnvVar) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum EventTimeSource {
    FromMetadata(EventTimeSourceFromMetadata),
    FromPath(EventTimeSourceFromPath),
    FromSystemTime(EventTimeSourceFromSystemTime),
}

impl From<odf::EventTimeSource> for EventTimeSource {
    fn from(v: odf::EventTimeSource) -> Self {
        match v {
            odf::EventTimeSource::FromMetadata(v) => Self::FromMetadata(v.into()),
            odf::EventTimeSource::FromPath(v) => Self::FromPath(v.into()),
            odf::EventTimeSource::FromSystemTime(v) => Self::FromSystemTime(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromMetadata {
    pub _dummy: Option<String>,
}

impl From<odf::EventTimeSourceFromMetadata> for EventTimeSourceFromMetadata {
    fn from(v: odf::EventTimeSourceFromMetadata) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct EventTimeSourceFromPath {
    pub pattern: String,
    pub timestamp_format: Option<String>,
}

impl From<odf::EventTimeSourceFromPath> for EventTimeSourceFromPath {
    fn from(v: odf::EventTimeSourceFromPath) -> Self {
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

impl From<odf::EventTimeSourceFromSystemTime> for EventTimeSourceFromSystemTime {
    fn from(v: odf::EventTimeSourceFromSystemTime) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQuery {
    pub query_inputs: Vec<ExecuteQueryInput>,
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_data: Option<DataSlice>,
    pub new_checkpoint: Option<Checkpoint>,
    pub new_watermark: Option<DateTime<Utc>>,
}

impl From<odf::ExecuteQuery> for ExecuteQuery {
    fn from(v: odf::ExecuteQuery) -> Self {
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

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryInput {
    pub dataset_id: DatasetID,
    pub prev_block_hash: Option<Multihash>,
    pub new_block_hash: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_offset: Option<u64>,
}

impl From<odf::ExecuteQueryInput> for ExecuteQueryInput {
    fn from(v: odf::ExecuteQueryInput) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            prev_block_hash: v.prev_block_hash.map(Into::into),
            new_block_hash: v.new_block_hash.map(Into::into),
            prev_offset: v.prev_offset.map(Into::into),
            new_offset: v.new_offset.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryRequest {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub system_time: DateTime<Utc>,
    pub vocab: DatasetVocabulary,
    pub transform: Transform,
    pub query_inputs: Vec<ExecuteQueryRequestInput>,
    pub next_offset: u64,
    pub prev_checkpoint_path: Option<OSPath>,
    pub new_checkpoint_path: OSPath,
    pub new_data_path: OSPath,
}

impl From<odf::ExecuteQueryRequest> for ExecuteQueryRequest {
    fn from(v: odf::ExecuteQueryRequest) -> Self {
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

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequestinput-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryRequestInput {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub query_alias: String,
    pub vocab: DatasetVocabulary,
    pub offset_interval: Option<OffsetInterval>,
    pub data_paths: Vec<OSPath>,
    pub schema_file: OSPath,
    pub explicit_watermarks: Vec<Watermark>,
}

impl From<odf::ExecuteQueryRequestInput> for ExecuteQueryRequestInput {
    fn from(v: odf::ExecuteQueryRequestInput) -> Self {
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

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryresponse-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum ExecuteQueryResponse {
    Progress(ExecuteQueryResponseProgress),
    Success(ExecuteQueryResponseSuccess),
    InvalidQuery(ExecuteQueryResponseInvalidQuery),
    InternalError(ExecuteQueryResponseInternalError),
}

impl From<odf::ExecuteQueryResponse> for ExecuteQueryResponse {
    fn from(v: odf::ExecuteQueryResponse) -> Self {
        match v {
            odf::ExecuteQueryResponse::Progress(v) => Self::Progress(v.into()),
            odf::ExecuteQueryResponse::Success(v) => Self::Success(v.into()),
            odf::ExecuteQueryResponse::InvalidQuery(v) => Self::InvalidQuery(v.into()),
            odf::ExecuteQueryResponse::InternalError(v) => Self::InternalError(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryResponseProgress {
    pub _dummy: Option<String>,
}

impl From<odf::ExecuteQueryResponseProgress> for ExecuteQueryResponseProgress {
    fn from(v: odf::ExecuteQueryResponseProgress) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryResponseSuccess {
    pub new_offset_interval: Option<OffsetInterval>,
    pub new_watermark: Option<DateTime<Utc>>,
}

impl From<odf::ExecuteQueryResponseSuccess> for ExecuteQueryResponseSuccess {
    fn from(v: odf::ExecuteQueryResponseSuccess) -> Self {
        Self {
            new_offset_interval: v.new_offset_interval.map(Into::into),
            new_watermark: v.new_watermark.map(Into::into),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryResponseInvalidQuery {
    pub message: String,
}

impl From<odf::ExecuteQueryResponseInvalidQuery> for ExecuteQueryResponseInvalidQuery {
    fn from(v: odf::ExecuteQueryResponseInvalidQuery) -> Self {
        Self {
            message: v.message.into(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct ExecuteQueryResponseInternalError {
    pub message: String,
    pub backtrace: Option<String>,
}

impl From<odf::ExecuteQueryResponseInternalError> for ExecuteQueryResponseInternalError {
    fn from(v: odf::ExecuteQueryResponseInternalError) -> Self {
        Self {
            message: v.message.into(),
            backtrace: v.backtrace.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum FetchStep {
    Url(FetchStepUrl),
    FilesGlob(FetchStepFilesGlob),
    Container(FetchStepContainer),
}

impl From<odf::FetchStep> for FetchStep {
    fn from(v: odf::FetchStep) -> Self {
        match v {
            odf::FetchStep::Url(v) => Self::Url(v.into()),
            odf::FetchStep::FilesGlob(v) => Self::FilesGlob(v.into()),
            odf::FetchStep::Container(v) => Self::Container(v.into()),
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

impl From<odf::FetchStepUrl> for FetchStepUrl {
    fn from(v: odf::FetchStepUrl) -> Self {
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

impl From<odf::FetchStepFilesGlob> for FetchStepFilesGlob {
    fn from(v: odf::FetchStepFilesGlob) -> Self {
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

impl From<odf::FetchStepContainer> for FetchStepContainer {
    fn from(v: odf::FetchStepContainer) -> Self {
        Self {
            image: v.image.into(),
            command: v.command.map(|v| v.into_iter().map(Into::into).collect()),
            args: v.args.map(|v| v.into_iter().map(Into::into).collect()),
            env: v.env.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceOrdering {
    ByEventTime,
    ByName,
}

impl From<odf::SourceOrdering> for SourceOrdering {
    fn from(v: odf::SourceOrdering) -> Self {
        match v {
            odf::SourceOrdering::ByEventTime => Self::ByEventTime,
            odf::SourceOrdering::ByName => Self::ByName,
        }
    }
}

impl Into<odf::SourceOrdering> for SourceOrdering {
    fn into(self) -> odf::SourceOrdering {
        match self {
            Self::ByEventTime => odf::SourceOrdering::ByEventTime,
            Self::ByName => odf::SourceOrdering::ByName,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum MergeStrategy {
    Append(MergeStrategyAppend),
    Ledger(MergeStrategyLedger),
    Snapshot(MergeStrategySnapshot),
}

impl From<odf::MergeStrategy> for MergeStrategy {
    fn from(v: odf::MergeStrategy) -> Self {
        match v {
            odf::MergeStrategy::Append(v) => Self::Append(v.into()),
            odf::MergeStrategy::Ledger(v) => Self::Ledger(v.into()),
            odf::MergeStrategy::Snapshot(v) => Self::Snapshot(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyAppend {
    pub _dummy: Option<String>,
}

impl From<odf::MergeStrategyAppend> for MergeStrategyAppend {
    fn from(v: odf::MergeStrategyAppend) -> Self {
        Self { _dummy: None }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategyLedger {
    pub primary_key: Vec<String>,
}

impl From<odf::MergeStrategyLedger> for MergeStrategyLedger {
    fn from(v: odf::MergeStrategyLedger) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MergeStrategySnapshot {
    pub primary_key: Vec<String>,
    pub compare_columns: Option<Vec<String>>,
    pub observation_column: Option<String>,
    pub obsv_added: Option<String>,
    pub obsv_changed: Option<String>,
    pub obsv_removed: Option<String>,
}

impl From<odf::MergeStrategySnapshot> for MergeStrategySnapshot {
    fn from(v: odf::MergeStrategySnapshot) -> Self {
        Self {
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
            compare_columns: v
                .compare_columns
                .map(|v| v.into_iter().map(Into::into).collect()),
            observation_column: v.observation_column.map(Into::into),
            obsv_added: v.obsv_added.map(Into::into),
            obsv_changed: v.obsv_changed.map(Into::into),
            obsv_removed: v.obsv_removed.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlock {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub sequence_number: u64,
    pub event: MetadataEvent,
}

impl From<odf::MetadataBlock> for MetadataBlock {
    fn from(v: odf::MetadataBlock) -> Self {
        Self {
            system_time: v.system_time.into(),
            prev_block_hash: v.prev_block_hash.map(Into::into),
            sequence_number: v.sequence_number.into(),
            event: v.event.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
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
    SetDataSchema(SetDataSchema),
    AddPushSource(AddPushSource),
    DisablePushSource(DisablePushSource),
    DisablePollingSource(DisablePollingSource),
}

impl From<odf::MetadataEvent> for MetadataEvent {
    fn from(v: odf::MetadataEvent) -> Self {
        match v {
            odf::MetadataEvent::AddData(v) => Self::AddData(v.into()),
            odf::MetadataEvent::ExecuteQuery(v) => Self::ExecuteQuery(v.into()),
            odf::MetadataEvent::Seed(v) => Self::Seed(v.into()),
            odf::MetadataEvent::SetPollingSource(v) => Self::SetPollingSource(v.into()),
            odf::MetadataEvent::SetTransform(v) => Self::SetTransform(v.into()),
            odf::MetadataEvent::SetVocab(v) => Self::SetVocab(v.into()),
            odf::MetadataEvent::SetWatermark(v) => Self::SetWatermark(v.into()),
            odf::MetadataEvent::SetAttachments(v) => Self::SetAttachments(v.into()),
            odf::MetadataEvent::SetInfo(v) => Self::SetInfo(v.into()),
            odf::MetadataEvent::SetLicense(v) => Self::SetLicense(v.into()),
            odf::MetadataEvent::SetDataSchema(v) => Self::SetDataSchema(v.into()),
            odf::MetadataEvent::AddPushSource(v) => Self::AddPushSource(v.into()),
            odf::MetadataEvent::DisablePushSource(v) => Self::DisablePushSource(v.into()),
            odf::MetadataEvent::DisablePollingSource(v) => Self::DisablePollingSource(v.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct OffsetInterval {
    pub start: u64,
    pub end: u64,
}

impl From<odf::OffsetInterval> for OffsetInterval {
    fn from(v: odf::OffsetInterval) -> Self {
        Self {
            start: v.start.into(),
            end: v.end.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum PrepStep {
    Decompress(PrepStepDecompress),
    Pipe(PrepStepPipe),
}

impl From<odf::PrepStep> for PrepStep {
    fn from(v: odf::PrepStep) -> Self {
        match v {
            odf::PrepStep::Decompress(v) => Self::Decompress(v.into()),
            odf::PrepStep::Pipe(v) => Self::Pipe(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct PrepStepDecompress {
    pub format: CompressionFormat,
    pub sub_path: Option<String>,
}

impl From<odf::PrepStepDecompress> for PrepStepDecompress {
    fn from(v: odf::PrepStepDecompress) -> Self {
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

impl From<odf::PrepStepPipe> for PrepStepPipe {
    fn from(v: odf::PrepStepPipe) -> Self {
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

impl From<odf::CompressionFormat> for CompressionFormat {
    fn from(v: odf::CompressionFormat) -> Self {
        match v {
            odf::CompressionFormat::Gzip => Self::Gzip,
            odf::CompressionFormat::Zip => Self::Zip,
        }
    }
}

impl Into<odf::CompressionFormat> for CompressionFormat {
    fn into(self) -> odf::CompressionFormat {
        match self {
            Self::Gzip => odf::CompressionFormat::Gzip,
            Self::Zip => odf::CompressionFormat::Zip,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

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

impl From<odf::ReadStep> for ReadStep {
    fn from(v: odf::ReadStep) -> Self {
        match v {
            odf::ReadStep::Csv(v) => Self::Csv(v.into()),
            odf::ReadStep::GeoJson(v) => Self::GeoJson(v.into()),
            odf::ReadStep::EsriShapefile(v) => Self::EsriShapefile(v.into()),
            odf::ReadStep::Parquet(v) => Self::Parquet(v.into()),
            odf::ReadStep::Json(v) => Self::Json(v.into()),
            odf::ReadStep::NdJson(v) => Self::NdJson(v.into()),
            odf::ReadStep::NdGeoJson(v) => Self::NdGeoJson(v.into()),
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

impl From<odf::ReadStepCsv> for ReadStepCsv {
    fn from(v: odf::ReadStepCsv) -> Self {
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

impl From<odf::ReadStepGeoJson> for ReadStepGeoJson {
    fn from(v: odf::ReadStepGeoJson) -> Self {
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

impl From<odf::ReadStepEsriShapefile> for ReadStepEsriShapefile {
    fn from(v: odf::ReadStepEsriShapefile) -> Self {
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

impl From<odf::ReadStepParquet> for ReadStepParquet {
    fn from(v: odf::ReadStepParquet) -> Self {
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

impl From<odf::ReadStepJson> for ReadStepJson {
    fn from(v: odf::ReadStepJson) -> Self {
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

impl From<odf::ReadStepNdJson> for ReadStepNdJson {
    fn from(v: odf::ReadStepNdJson) -> Self {
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

impl From<odf::ReadStepNdGeoJson> for ReadStepNdGeoJson {
    fn from(v: odf::ReadStepNdGeoJson) -> Self {
        Self {
            schema: v.schema.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub name: String,
    pub value: String,
}

impl From<odf::RequestHeader> for RequestHeader {
    fn from(v: odf::RequestHeader) -> Self {
        Self {
            name: v.name.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Seed {
    pub dataset_id: DatasetID,
    pub dataset_kind: DatasetKind,
}

impl From<odf::Seed> for Seed {
    fn from(v: odf::Seed) -> Self {
        Self {
            dataset_id: v.dataset_id.into(),
            dataset_kind: v.dataset_kind.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetAttachments {
    pub attachments: Attachments,
}

impl From<odf::SetAttachments> for SetAttachments {
    fn from(v: odf::SetAttachments) -> Self {
        Self {
            attachments: v.attachments.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetDataSchema {
    pub schema: DataSchema,
}

impl From<odf::SetDataSchema> for SetDataSchema {
    fn from(v: odf::SetDataSchema) -> Self {
        // TODO: Error handling?
        // TODO: Externalize format decision?
        let arrow_schema = v.schema_as_arrow().unwrap();
        let schema =
            DataSchema::from_arrow_schema(&arrow_schema, DataSchemaFormat::ParquetJson).unwrap();
        Self { schema }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetInfo {
    pub description: Option<String>,
    pub keywords: Option<Vec<String>>,
}

impl From<odf::SetInfo> for SetInfo {
    fn from(v: odf::SetInfo) -> Self {
        Self {
            description: v.description.map(Into::into),
            keywords: v.keywords.map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetLicense {
    pub short_name: String,
    pub name: String,
    pub spdx_id: Option<String>,
    pub website_url: String,
}

impl From<odf::SetLicense> for SetLicense {
    fn from(v: odf::SetLicense) -> Self {
        Self {
            short_name: v.short_name.into(),
            name: v.name.into(),
            spdx_id: v.spdx_id.map(Into::into),
            website_url: v.website_url.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetPollingSource {
    pub fetch: FetchStep,
    pub prepare: Option<Vec<PrepStep>>,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

impl From<odf::SetPollingSource> for SetPollingSource {
    fn from(v: odf::SetPollingSource) -> Self {
        Self {
            fetch: v.fetch.into(),
            prepare: v.prepare.map(|v| v.into_iter().map(Into::into).collect()),
            read: v.read.into(),
            preprocess: v.preprocess.map(Into::into),
            merge: v.merge.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetTransform {
    pub inputs: Vec<TransformInput>,
    pub transform: Transform,
}

impl From<odf::SetTransform> for SetTransform {
    fn from(v: odf::SetTransform) -> Self {
        Self {
            inputs: v.inputs.into_iter().map(Into::into).collect(),
            transform: v.transform.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetVocab {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
}

impl From<odf::SetVocab> for SetVocab {
    fn from(v: odf::SetVocab) -> Self {
        Self {
            system_time_column: v.system_time_column.map(Into::into),
            event_time_column: v.event_time_column.map(Into::into),
            offset_column: v.offset_column.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SetWatermark {
    pub new_watermark: DateTime<Utc>,
}

impl From<odf::SetWatermark> for SetWatermark {
    fn from(v: odf::SetWatermark) -> Self {
        Self {
            new_watermark: v.new_watermark.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum SourceCaching {
    Forever(SourceCachingForever),
}

impl From<odf::SourceCaching> for SourceCaching {
    fn from(v: odf::SourceCaching) -> Self {
        match v {
            odf::SourceCaching::Forever(v) => Self::Forever(v.into()),
        }
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SourceCachingForever {
    pub _dummy: Option<String>,
}

impl From<odf::SourceCachingForever> for SourceCachingForever {
    fn from(v: odf::SourceCachingForever) -> Self {
        Self { _dummy: None }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SourceState {
    pub source_name: Option<String>,
    pub kind: String,
    pub value: String,
}

impl From<odf::SourceState> for SourceState {
    fn from(v: odf::SourceState) -> Self {
        Self {
            source_name: v.source_name.map(Into::into),
            kind: v.kind.into(),
            value: v.value.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct SqlQueryStep {
    pub alias: Option<String>,
    pub query: String,
}

impl From<odf::SqlQueryStep> for SqlQueryStep {
    fn from(v: odf::SqlQueryStep) -> Self {
        Self {
            alias: v.alias.map(Into::into),
            query: v.query.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TemporalTable {
    pub name: String,
    pub primary_key: Vec<String>,
}

impl From<odf::TemporalTable> for TemporalTable {
    fn from(v: odf::TemporalTable) -> Self {
        Self {
            name: v.name.into(),
            primary_key: v.primary_key.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    Sql(TransformSql),
}

impl From<odf::Transform> for Transform {
    fn from(v: odf::Transform) -> Self {
        match v {
            odf::Transform::Sql(v) => Self::Sql(v.into()),
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

impl From<odf::TransformSql> for TransformSql {
    fn from(v: odf::TransformSql) -> Self {
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

////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
#[graphql(complex)]
pub struct TransformInput {
    pub dataset_ref: DatasetRef,
    pub alias: String,
}

#[ComplexObject]
impl TransformInput {
    async fn dataset(&self, ctx: &Context<'_>) -> Result<Dataset> {
        Dataset::from_ref(ctx, &self.dataset_ref).await
    }
}

impl From<odf::TransformInput> for TransformInput {
    fn from(v: odf::TransformInput) -> Self {
        Self {
            dataset_ref: v.dataset_ref.into(),
            alias: v.alias.unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub system_time: DateTime<Utc>,
    pub event_time: DateTime<Utc>,
}

impl From<odf::Watermark> for Watermark {
    fn from(v: odf::Watermark) -> Self {
        Self {
            system_time: v.system_time.into(),
            event_time: v.event_time.into(),
        }
    }
}
