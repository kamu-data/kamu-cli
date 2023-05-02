// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::*;
use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use chrono::{DateTime, Utc};
use serde_with::serde_as;
use serde_with::skip_serializing_none;
use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////

macro_rules! implement_serde_as {
    ($dto:ty, $impl:ty, $impl_name:literal) => {
        impl ::serde_with::SerializeAs<$dto> for $impl {
            fn serialize_as<S>(source: &$dto, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                <$impl>::serialize(source, serializer)
            }
        }

        impl<'de> serde_with::DeserializeAs<'de, $dto> for $impl {
            fn deserialize_as<D>(deserializer: D) -> Result<$dto, D::Error>
            where
                D: Deserializer<'de>,
            {
                <$impl>::deserialize(deserializer)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "AddData")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddDataDef {
    pub input_checkpoint: Option<Multihash>,
    #[serde_as(as = "Option<DataSliceDef>")]
    #[serde(default)]
    pub output_data: Option<DataSlice>,
    #[serde_as(as = "Option<CheckpointDef>")]
    #[serde(default)]
    pub output_checkpoint: Option<Checkpoint>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub output_watermark: Option<DateTime<Utc>>,
    #[serde_as(as = "Option<SourceStateDef>")]
    #[serde(default)]
    pub source_state: Option<SourceState>,
}

implement_serde_as!(AddData, AddDataDef, "AddDataDef");

////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "AttachmentEmbedded")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AttachmentEmbeddedDef {
    pub path: String,
    pub content: String,
}

implement_serde_as!(
    AttachmentEmbedded,
    AttachmentEmbeddedDef,
    "AttachmentEmbeddedDef"
);

////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Attachments")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum AttachmentsDef {
    #[serde(rename_all = "camelCase")]
    Embedded(#[serde_as(as = "AttachmentsEmbeddedDef")] AttachmentsEmbedded),
}

implement_serde_as!(Attachments, AttachmentsDef, "AttachmentsDef");
implement_serde_as!(
    AttachmentsEmbedded,
    AttachmentsEmbeddedDef,
    "AttachmentsEmbeddedDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "AttachmentsEmbedded")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AttachmentsEmbeddedDef {
    #[serde_as(as = "Vec<AttachmentEmbeddedDef>")]
    pub items: Vec<AttachmentEmbedded>,
}

////////////////////////////////////////////////////////////////////////////////
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "BlockInterval")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BlockIntervalDef {
    pub start: Multihash,
    pub end: Multihash,
}

implement_serde_as!(BlockInterval, BlockIntervalDef, "BlockIntervalDef");

////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Checkpoint")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointDef {
    pub physical_hash: Multihash,
    pub size: i64,
}

implement_serde_as!(Checkpoint, CheckpointDef, "CheckpointDef");

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DataSlice")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DataSliceDef {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    #[serde_as(as = "OffsetIntervalDef")]
    pub interval: OffsetInterval,
    pub size: i64,
}

implement_serde_as!(DataSlice, DataSliceDef, "DataSliceDef");

////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetKind")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum DatasetKindDef {
    Root,
    Derivative,
}

implement_serde_as!(DatasetKind, DatasetKindDef, "DatasetKindDef");

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSnapshot")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSnapshotDef {
    pub name: DatasetName,
    #[serde_as(as = "DatasetKindDef")]
    pub kind: DatasetKind,
    #[serde_as(as = "Vec<MetadataEventDef>")]
    pub metadata: Vec<MetadataEvent>,
}

implement_serde_as!(DatasetSnapshot, DatasetSnapshotDef, "DatasetSnapshotDef");

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetVocabulary")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetVocabularyDef {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
}

implement_serde_as!(
    DatasetVocabulary,
    DatasetVocabularyDef,
    "DatasetVocabularyDef"
);

////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EnvVar")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnvVarDef {
    pub name: String,
    pub value: Option<String>,
}

implement_serde_as!(EnvVar, EnvVarDef, "EnvVarDef");

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum EventTimeSourceDef {
    #[serde(rename_all = "camelCase")]
    FromMetadata,
    #[serde(rename_all = "camelCase")]
    FromPath(#[serde_as(as = "EventTimeSourceFromPathDef")] EventTimeSourceFromPath),
}

implement_serde_as!(EventTimeSource, EventTimeSourceDef, "EventTimeSourceDef");
implement_serde_as!(
    EventTimeSourceFromPath,
    EventTimeSourceFromPathDef,
    "EventTimeSourceFromPathDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSourceFromPath")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventTimeSourceFromPathDef {
    pub pattern: String,
    pub timestamp_format: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQuery")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryDef {
    #[serde_as(as = "Vec<InputSliceDef>")]
    pub input_slices: Vec<InputSlice>,
    pub input_checkpoint: Option<Multihash>,
    #[serde_as(as = "Option<DataSliceDef>")]
    #[serde(default)]
    pub output_data: Option<DataSlice>,
    #[serde_as(as = "Option<CheckpointDef>")]
    #[serde(default)]
    pub output_checkpoint: Option<Checkpoint>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub output_watermark: Option<DateTime<Utc>>,
}

implement_serde_as!(ExecuteQuery, ExecuteQueryDef, "ExecuteQueryDef");

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryInputDef {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    #[serde_as(as = "Option<OffsetIntervalDef>")]
    #[serde(default)]
    pub data_interval: Option<OffsetInterval>,
    pub data_paths: Vec<PathBuf>,
    pub schema_file: PathBuf,
    #[serde_as(as = "Vec<WatermarkDef>")]
    pub explicit_watermarks: Vec<Watermark>,
}

implement_serde_as!(
    ExecuteQueryInput,
    ExecuteQueryInputDef,
    "ExecuteQueryInputDef"
);

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryRequest")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryRequestDef {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    pub offset: i64,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
    #[serde_as(as = "Vec<ExecuteQueryInputDef>")]
    pub inputs: Vec<ExecuteQueryInput>,
    pub prev_checkpoint_path: Option<PathBuf>,
    pub new_checkpoint_path: PathBuf,
    pub out_data_path: PathBuf,
}

implement_serde_as!(
    ExecuteQueryRequest,
    ExecuteQueryRequestDef,
    "ExecuteQueryRequestDef"
);

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryresponse-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryResponse")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum ExecuteQueryResponseDef {
    #[serde(rename_all = "camelCase")]
    Progress,
    #[serde(rename_all = "camelCase")]
    Success(#[serde_as(as = "ExecuteQueryResponseSuccessDef")] ExecuteQueryResponseSuccess),
    #[serde(rename_all = "camelCase")]
    InvalidQuery(
        #[serde_as(as = "ExecuteQueryResponseInvalidQueryDef")] ExecuteQueryResponseInvalidQuery,
    ),
    #[serde(rename_all = "camelCase")]
    InternalError(
        #[serde_as(as = "ExecuteQueryResponseInternalErrorDef")] ExecuteQueryResponseInternalError,
    ),
}

implement_serde_as!(
    ExecuteQueryResponse,
    ExecuteQueryResponseDef,
    "ExecuteQueryResponseDef"
);
implement_serde_as!(
    ExecuteQueryResponseSuccess,
    ExecuteQueryResponseSuccessDef,
    "ExecuteQueryResponseSuccessDef"
);
implement_serde_as!(
    ExecuteQueryResponseInvalidQuery,
    ExecuteQueryResponseInvalidQueryDef,
    "ExecuteQueryResponseInvalidQueryDef"
);
implement_serde_as!(
    ExecuteQueryResponseInternalError,
    ExecuteQueryResponseInternalErrorDef,
    "ExecuteQueryResponseInternalErrorDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryResponseSuccess")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryResponseSuccessDef {
    #[serde_as(as = "Option<OffsetIntervalDef>")]
    #[serde(default)]
    pub data_interval: Option<OffsetInterval>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub output_watermark: Option<DateTime<Utc>>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryResponseInvalidQuery")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryResponseInvalidQueryDef {
    pub message: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteQueryResponseInternalError")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryResponseInternalErrorDef {
    pub message: String,
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum FetchStepDef {
    #[serde(rename_all = "camelCase")]
    Url(#[serde_as(as = "FetchStepUrlDef")] FetchStepUrl),
    #[serde(rename_all = "camelCase")]
    FilesGlob(#[serde_as(as = "FetchStepFilesGlobDef")] FetchStepFilesGlob),
    #[serde(rename_all = "camelCase")]
    Container(#[serde_as(as = "FetchStepContainerDef")] FetchStepContainer),
}

implement_serde_as!(FetchStep, FetchStepDef, "FetchStepDef");
implement_serde_as!(FetchStepUrl, FetchStepUrlDef, "FetchStepUrlDef");
implement_serde_as!(
    FetchStepFilesGlob,
    FetchStepFilesGlobDef,
    "FetchStepFilesGlobDef"
);
implement_serde_as!(
    FetchStepContainer,
    FetchStepContainerDef,
    "FetchStepContainerDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStepUrl")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchStepUrlDef {
    pub url: String,
    #[serde_as(as = "Option<EventTimeSourceDef>")]
    #[serde(default)]
    pub event_time: Option<EventTimeSource>,
    #[serde_as(as = "Option<SourceCachingDef>")]
    #[serde(default)]
    pub cache: Option<SourceCaching>,
    #[serde_as(as = "Option<Vec<RequestHeaderDef>>")]
    #[serde(default)]
    pub headers: Option<Vec<RequestHeader>>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStepFilesGlob")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchStepFilesGlobDef {
    pub path: String,
    #[serde_as(as = "Option<EventTimeSourceDef>")]
    #[serde(default)]
    pub event_time: Option<EventTimeSource>,
    #[serde_as(as = "Option<SourceCachingDef>")]
    #[serde(default)]
    pub cache: Option<SourceCaching>,
    #[serde_as(as = "Option<SourceOrderingDef>")]
    #[serde(default)]
    pub order: Option<SourceOrdering>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStepContainer")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchStepContainerDef {
    pub image: String,
    pub command: Option<Vec<String>>,
    pub args: Option<Vec<String>>,
    #[serde_as(as = "Option<Vec<EnvVarDef>>")]
    #[serde(default)]
    pub env: Option<Vec<EnvVar>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceOrdering")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum SourceOrderingDef {
    ByEventTime,
    ByName,
}

implement_serde_as!(SourceOrdering, SourceOrderingDef, "SourceOrderingDef");

////////////////////////////////////////////////////////////////////////////////
// InputSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#inputslice-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "InputSlice")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputSliceDef {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    #[serde_as(as = "Option<BlockIntervalDef>")]
    #[serde(default)]
    pub block_interval: Option<BlockInterval>,
    #[serde_as(as = "Option<OffsetIntervalDef>")]
    #[serde(default)]
    pub data_interval: Option<OffsetInterval>,
}

implement_serde_as!(InputSlice, InputSliceDef, "InputSliceDef");

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MergeStrategy")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum MergeStrategyDef {
    #[serde(rename_all = "camelCase")]
    Append,
    #[serde(rename_all = "camelCase")]
    Ledger(#[serde_as(as = "MergeStrategyLedgerDef")] MergeStrategyLedger),
    #[serde(rename_all = "camelCase")]
    Snapshot(#[serde_as(as = "MergeStrategySnapshotDef")] MergeStrategySnapshot),
}

implement_serde_as!(MergeStrategy, MergeStrategyDef, "MergeStrategyDef");
implement_serde_as!(
    MergeStrategyLedger,
    MergeStrategyLedgerDef,
    "MergeStrategyLedgerDef"
);
implement_serde_as!(
    MergeStrategySnapshot,
    MergeStrategySnapshotDef,
    "MergeStrategySnapshotDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MergeStrategyLedger")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergeStrategyLedgerDef {
    pub primary_key: Vec<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MergeStrategySnapshot")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergeStrategySnapshotDef {
    pub primary_key: Vec<String>,
    pub compare_columns: Option<Vec<String>>,
    pub observation_column: Option<String>,
    pub obsv_added: Option<String>,
    pub obsv_changed: Option<String>,
    pub obsv_removed: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MetadataBlock")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MetadataBlockDef {
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub sequence_number: i32,
    #[serde_as(as = "MetadataEventDef")]
    pub event: MetadataEvent,
}

implement_serde_as!(MetadataBlock, MetadataBlockDef, "MetadataBlockDef");

////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MetadataEvent")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum MetadataEventDef {
    #[serde(rename_all = "camelCase")]
    AddData(#[serde_as(as = "AddDataDef")] AddData),
    #[serde(rename_all = "camelCase")]
    ExecuteQuery(#[serde_as(as = "ExecuteQueryDef")] ExecuteQuery),
    #[serde(rename_all = "camelCase")]
    Seed(#[serde_as(as = "SeedDef")] Seed),
    #[serde(rename_all = "camelCase")]
    SetPollingSource(#[serde_as(as = "SetPollingSourceDef")] SetPollingSource),
    #[serde(rename_all = "camelCase")]
    SetTransform(#[serde_as(as = "SetTransformDef")] SetTransform),
    #[serde(rename_all = "camelCase")]
    SetVocab(#[serde_as(as = "SetVocabDef")] SetVocab),
    #[serde(rename_all = "camelCase")]
    SetWatermark(#[serde_as(as = "SetWatermarkDef")] SetWatermark),
    #[serde(rename_all = "camelCase")]
    SetAttachments(#[serde_as(as = "SetAttachmentsDef")] SetAttachments),
    #[serde(rename_all = "camelCase")]
    SetInfo(#[serde_as(as = "SetInfoDef")] SetInfo),
    #[serde(rename_all = "camelCase")]
    SetLicense(#[serde_as(as = "SetLicenseDef")] SetLicense),
}

implement_serde_as!(MetadataEvent, MetadataEventDef, "MetadataEventDef");

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "OffsetInterval")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OffsetIntervalDef {
    pub start: i64,
    pub end: i64,
}

implement_serde_as!(OffsetInterval, OffsetIntervalDef, "OffsetIntervalDef");

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "PrepStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum PrepStepDef {
    #[serde(rename_all = "camelCase")]
    Decompress(#[serde_as(as = "PrepStepDecompressDef")] PrepStepDecompress),
    #[serde(rename_all = "camelCase")]
    Pipe(#[serde_as(as = "PrepStepPipeDef")] PrepStepPipe),
}

implement_serde_as!(PrepStep, PrepStepDef, "PrepStepDef");
implement_serde_as!(
    PrepStepDecompress,
    PrepStepDecompressDef,
    "PrepStepDecompressDef"
);
implement_serde_as!(PrepStepPipe, PrepStepPipeDef, "PrepStepPipeDef");

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "PrepStepDecompress")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrepStepDecompressDef {
    #[serde_as(as = "CompressionFormatDef")]
    pub format: CompressionFormat,
    pub sub_path: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "PrepStepPipe")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrepStepPipeDef {
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "CompressionFormat")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CompressionFormatDef {
    Gzip,
    Zip,
}

implement_serde_as!(
    CompressionFormat,
    CompressionFormatDef,
    "CompressionFormatDef"
);

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum ReadStepDef {
    #[serde(rename_all = "camelCase")]
    Csv(#[serde_as(as = "ReadStepCsvDef")] ReadStepCsv),
    #[serde(rename_all = "camelCase")]
    JsonLines(#[serde_as(as = "ReadStepJsonLinesDef")] ReadStepJsonLines),
    #[serde(rename_all = "camelCase")]
    GeoJson(#[serde_as(as = "ReadStepGeoJsonDef")] ReadStepGeoJson),
    #[serde(rename_all = "camelCase")]
    EsriShapefile(#[serde_as(as = "ReadStepEsriShapefileDef")] ReadStepEsriShapefile),
    #[serde(rename_all = "camelCase")]
    Parquet(#[serde_as(as = "ReadStepParquetDef")] ReadStepParquet),
}

implement_serde_as!(ReadStep, ReadStepDef, "ReadStepDef");
implement_serde_as!(ReadStepCsv, ReadStepCsvDef, "ReadStepCsvDef");
implement_serde_as!(
    ReadStepJsonLines,
    ReadStepJsonLinesDef,
    "ReadStepJsonLinesDef"
);
implement_serde_as!(ReadStepGeoJson, ReadStepGeoJsonDef, "ReadStepGeoJsonDef");
implement_serde_as!(
    ReadStepEsriShapefile,
    ReadStepEsriShapefileDef,
    "ReadStepEsriShapefileDef"
);
implement_serde_as!(ReadStepParquet, ReadStepParquetDef, "ReadStepParquetDef");

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepCsv")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepCsvDef {
    pub schema: Option<Vec<String>>,
    pub separator: Option<String>,
    pub encoding: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub comment: Option<String>,
    pub header: Option<bool>,
    pub enforce_schema: Option<bool>,
    pub infer_schema: Option<bool>,
    pub ignore_leading_white_space: Option<bool>,
    pub ignore_trailing_white_space: Option<bool>,
    pub null_value: Option<String>,
    pub empty_value: Option<String>,
    pub nan_value: Option<String>,
    pub positive_inf: Option<String>,
    pub negative_inf: Option<String>,
    pub date_format: Option<String>,
    pub timestamp_format: Option<String>,
    pub multi_line: Option<bool>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepJsonLines")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepJsonLinesDef {
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
    pub multi_line: Option<bool>,
    pub primitives_as_string: Option<bool>,
    pub timestamp_format: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepGeoJson")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepGeoJsonDef {
    pub schema: Option<Vec<String>>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepEsriShapefile")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepEsriShapefileDef {
    pub schema: Option<Vec<String>>,
    pub sub_path: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepParquet")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepParquetDef {
    pub schema: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RequestHeader")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RequestHeaderDef {
    pub name: String,
    pub value: String,
}

implement_serde_as!(RequestHeader, RequestHeaderDef, "RequestHeaderDef");

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Seed")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SeedDef {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    #[serde_as(as = "DatasetKindDef")]
    pub dataset_kind: DatasetKind,
}

implement_serde_as!(Seed, SeedDef, "SeedDef");

////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetAttachments")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetAttachmentsDef {
    #[serde_as(as = "AttachmentsDef")]
    pub attachments: Attachments,
}

implement_serde_as!(SetAttachments, SetAttachmentsDef, "SetAttachmentsDef");

////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetInfo")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetInfoDef {
    pub description: Option<String>,
    pub keywords: Option<Vec<String>>,
}

implement_serde_as!(SetInfo, SetInfoDef, "SetInfoDef");

////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetLicense")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetLicenseDef {
    pub short_name: String,
    pub name: String,
    pub spdx_id: Option<String>,
    pub website_url: String,
}

implement_serde_as!(SetLicense, SetLicenseDef, "SetLicenseDef");

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetPollingSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetPollingSourceDef {
    #[serde_as(as = "FetchStepDef")]
    pub fetch: FetchStep,
    #[serde_as(as = "Option<Vec<PrepStepDef>>")]
    #[serde(default)]
    pub prepare: Option<Vec<PrepStep>>,
    #[serde_as(as = "ReadStepDef")]
    pub read: ReadStep,
    #[serde_as(as = "Option<TransformDef>")]
    #[serde(default)]
    pub preprocess: Option<Transform>,
    #[serde_as(as = "MergeStrategyDef")]
    pub merge: MergeStrategy,
}

implement_serde_as!(SetPollingSource, SetPollingSourceDef, "SetPollingSourceDef");

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetTransform")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetTransformDef {
    #[serde_as(as = "Vec<TransformInputDef>")]
    pub inputs: Vec<TransformInput>,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
}

implement_serde_as!(SetTransform, SetTransformDef, "SetTransformDef");

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetVocab")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetVocabDef {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
}

implement_serde_as!(SetVocab, SetVocabDef, "SetVocabDef");

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetWatermark")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetWatermarkDef {
    #[serde(with = "datetime_rfc3339")]
    pub output_watermark: DateTime<Utc>,
}

implement_serde_as!(SetWatermark, SetWatermarkDef, "SetWatermarkDef");

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceCaching")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum SourceCachingDef {
    #[serde(rename_all = "camelCase")]
    Forever,
}

implement_serde_as!(SourceCaching, SourceCachingDef, "SourceCachingDef");

////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceState")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceStateDef {
    pub kind: String,
    pub source: String,
    pub value: String,
}

implement_serde_as!(SourceState, SourceStateDef, "SourceStateDef");

////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SqlQueryStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SqlQueryStepDef {
    pub alias: Option<String>,
    pub query: String,
}

implement_serde_as!(SqlQueryStep, SqlQueryStepDef, "SqlQueryStepDef");

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TemporalTable")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TemporalTableDef {
    pub name: String,
    pub primary_key: Vec<String>,
}

implement_serde_as!(TemporalTable, TemporalTableDef, "TemporalTableDef");

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Transform")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum TransformDef {
    #[serde(rename_all = "camelCase")]
    Sql(#[serde_as(as = "TransformSqlDef")] TransformSql),
}

implement_serde_as!(Transform, TransformDef, "TransformDef");
implement_serde_as!(TransformSql, TransformSqlDef, "TransformSqlDef");

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformSql")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformSqlDef {
    pub engine: String,
    pub version: Option<String>,
    pub query: Option<String>,
    #[serde_as(as = "Option<Vec<SqlQueryStepDef>>")]
    #[serde(default)]
    pub queries: Option<Vec<SqlQueryStep>>,
    #[serde_as(as = "Option<Vec<TemporalTableDef>>")]
    #[serde(default)]
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformInputDef {
    pub id: Option<DatasetID>,
    pub name: DatasetName,
}

implement_serde_as!(TransformInput, TransformInputDef, "TransformInputDef");

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Watermark")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WatermarkDef {
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339")]
    pub event_time: DateTime<Utc>,
}

implement_serde_as!(Watermark, WatermarkDef, "WatermarkDef");
