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

use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use chrono::{DateTime, Utc};
use serde_with::{serde_as, skip_serializing_none};

use super::formats::{base64, datetime_rfc3339, datetime_rfc3339_opt};
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "AddData")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddDataDef {
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    #[serde_as(as = "Option<DataSliceDef>")]
    #[serde(default)]
    pub new_data: Option<DataSlice>,
    #[serde_as(as = "Option<CheckpointDef>")]
    #[serde(default)]
    pub new_checkpoint: Option<Checkpoint>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
    #[serde_as(as = "Option<SourceStateDef>")]
    #[serde(default)]
    pub new_source_state: Option<SourceState>,
}

implement_serde_as!(AddData, AddDataDef, "AddDataDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AddPushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#addpushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "AddPushSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddPushSourceDef {
    pub source_name: String,
    #[serde_as(as = "ReadStepDef")]
    pub read: ReadStep,
    #[serde_as(as = "Option<TransformDef>")]
    #[serde(default)]
    pub preprocess: Option<Transform>,
    #[serde_as(as = "MergeStrategyDef")]
    pub merge: MergeStrategy,
}

implement_serde_as!(AddPushSource, AddPushSourceDef, "AddPushSourceDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AttachmentEmbedded
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachmentembedded-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#attachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Attachments")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum AttachmentsDef {
    #[serde(alias = "embedded")]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Checkpoint
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#checkpoint-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Checkpoint")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointDef {
    pub physical_hash: Multihash,
    pub size: u64,
}

implement_serde_as!(Checkpoint, CheckpointDef, "CheckpointDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompressionFormat
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#compressionformat-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "CompressionFormat")]
#[serde(deny_unknown_fields)]
pub enum CompressionFormatDef {
    #[serde(alias = "gzip")]
    Gzip,
    #[serde(alias = "zip")]
    Zip,
}

implement_serde_as!(
    CompressionFormat,
    CompressionFormatDef,
    "CompressionFormatDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DataSlice")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DataSliceDef {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    #[serde_as(as = "OffsetIntervalDef")]
    pub offset_interval: OffsetInterval,
    pub size: u64,
}

implement_serde_as!(DataSlice, DataSliceDef, "DataSliceDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetkind-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetKind")]
#[serde(deny_unknown_fields)]
pub enum DatasetKindDef {
    #[serde(alias = "root")]
    Root,
    #[serde(alias = "derivative")]
    Derivative,
}

implement_serde_as!(DatasetKind, DatasetKindDef, "DatasetKindDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSnapshot")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSnapshotDef {
    pub name: DatasetAlias,
    #[serde_as(as = "DatasetKindDef")]
    pub kind: DatasetKind,
    #[serde_as(as = "Vec<MetadataEventDef>")]
    pub metadata: Vec<MetadataEvent>,
}

implement_serde_as!(DatasetSnapshot, DatasetSnapshotDef, "DatasetSnapshotDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetVocabulary")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetVocabularyDef {
    pub offset_column: String,
    pub operation_type_column: String,
    pub system_time_column: String,
    pub event_time_column: String,
}

implement_serde_as!(
    DatasetVocabulary,
    DatasetVocabularyDef,
    "DatasetVocabularyDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DisablePollingSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DisablePollingSourceDef {}

implement_serde_as!(
    DisablePollingSource,
    DisablePollingSourceDef,
    "DisablePollingSourceDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DisablePushSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#disablepushsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DisablePushSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DisablePushSourceDef {
    pub source_name: String,
}

implement_serde_as!(
    DisablePushSource,
    DisablePushSourceDef,
    "DisablePushSourceDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EnvVar
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#envvar-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSource")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum EventTimeSourceDef {
    #[serde(alias = "fromMetadata", alias = "frommetadata")]
    FromMetadata(#[serde_as(as = "EventTimeSourceFromMetadataDef")] EventTimeSourceFromMetadata),
    #[serde(alias = "fromPath", alias = "frompath")]
    FromPath(#[serde_as(as = "EventTimeSourceFromPathDef")] EventTimeSourceFromPath),
    #[serde(alias = "fromSystemTime", alias = "fromsystemtime")]
    FromSystemTime(
        #[serde_as(as = "EventTimeSourceFromSystemTimeDef")] EventTimeSourceFromSystemTime,
    ),
}

implement_serde_as!(EventTimeSource, EventTimeSourceDef, "EventTimeSourceDef");
implement_serde_as!(
    EventTimeSourceFromMetadata,
    EventTimeSourceFromMetadataDef,
    "EventTimeSourceFromMetadataDef"
);
implement_serde_as!(
    EventTimeSourceFromSystemTime,
    EventTimeSourceFromSystemTimeDef,
    "EventTimeSourceFromSystemTimeDef"
);
implement_serde_as!(
    EventTimeSourceFromPath,
    EventTimeSourceFromPathDef,
    "EventTimeSourceFromPathDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSourceFromMetadata")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventTimeSourceFromMetadataDef {}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSourceFromPath")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventTimeSourceFromPathDef {
    pub pattern: String,
    pub timestamp_format: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSourceFromSystemTime")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventTimeSourceFromSystemTimeDef {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteTransform")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteTransformDef {
    #[serde_as(as = "Vec<ExecuteTransformInputDef>")]
    pub query_inputs: Vec<ExecuteTransformInput>,
    pub prev_checkpoint: Option<Multihash>,
    pub prev_offset: Option<u64>,
    #[serde_as(as = "Option<DataSliceDef>")]
    #[serde(default)]
    pub new_data: Option<DataSlice>,
    #[serde_as(as = "Option<CheckpointDef>")]
    #[serde(default)]
    pub new_checkpoint: Option<Checkpoint>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
}

implement_serde_as!(ExecuteTransform, ExecuteTransformDef, "ExecuteTransformDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ExecuteTransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ExecuteTransformInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteTransformInputDef {
    pub dataset_id: DatasetID,
    pub prev_block_hash: Option<Multihash>,
    pub new_block_hash: Option<Multihash>,
    pub prev_offset: Option<u64>,
    pub new_offset: Option<u64>,
}

implement_serde_as!(
    ExecuteTransformInput,
    ExecuteTransformInputDef,
    "ExecuteTransformInputDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStep")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum FetchStepDef {
    #[serde(alias = "url")]
    Url(#[serde_as(as = "FetchStepUrlDef")] FetchStepUrl),
    #[serde(alias = "filesGlob", alias = "filesglob")]
    FilesGlob(#[serde_as(as = "FetchStepFilesGlobDef")] FetchStepFilesGlob),
    #[serde(alias = "container")]
    Container(#[serde_as(as = "FetchStepContainerDef")] FetchStepContainer),
    #[serde(alias = "mqtt")]
    Mqtt(#[serde_as(as = "FetchStepMqttDef")] FetchStepMqtt),
    #[serde(alias = "ethereumLogs", alias = "ethereumlogs")]
    EthereumLogs(#[serde_as(as = "FetchStepEthereumLogsDef")] FetchStepEthereumLogs),
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
implement_serde_as!(FetchStepMqtt, FetchStepMqttDef, "FetchStepMqttDef");
implement_serde_as!(
    FetchStepEthereumLogs,
    FetchStepEthereumLogsDef,
    "FetchStepEthereumLogsDef"
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

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStepMqtt")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchStepMqttDef {
    pub host: String,
    pub port: i32,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde_as(as = "Vec<MqttTopicSubscriptionDef>")]
    pub topics: Vec<MqttTopicSubscription>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStepEthereumLogs")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchStepEthereumLogsDef {
    pub chain_id: Option<u64>,
    pub node_url: Option<String>,
    pub filter: Option<String>,
    pub signature: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MergeStrategy")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum MergeStrategyDef {
    #[serde(alias = "append")]
    Append(#[serde_as(as = "MergeStrategyAppendDef")] MergeStrategyAppend),
    #[serde(alias = "ledger")]
    Ledger(#[serde_as(as = "MergeStrategyLedgerDef")] MergeStrategyLedger),
    #[serde(alias = "snapshot")]
    Snapshot(#[serde_as(as = "MergeStrategySnapshotDef")] MergeStrategySnapshot),
}

implement_serde_as!(MergeStrategy, MergeStrategyDef, "MergeStrategyDef");
implement_serde_as!(
    MergeStrategyAppend,
    MergeStrategyAppendDef,
    "MergeStrategyAppendDef"
);
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
#[serde(remote = "MergeStrategyAppend")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergeStrategyAppendDef {}

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MetadataBlock")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MetadataBlockDef {
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
    pub sequence_number: u64,
    #[serde_as(as = "MetadataEventDef")]
    pub event: MetadataEvent,
}

implement_serde_as!(MetadataBlock, MetadataBlockDef, "MetadataBlockDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataEvent
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadataevent-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MetadataEvent")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum MetadataEventDef {
    #[serde(alias = "addData", alias = "adddata")]
    AddData(#[serde_as(as = "AddDataDef")] AddData),
    #[serde(alias = "executeTransform", alias = "executetransform")]
    ExecuteTransform(#[serde_as(as = "ExecuteTransformDef")] ExecuteTransform),
    #[serde(alias = "seed")]
    Seed(#[serde_as(as = "SeedDef")] Seed),
    #[serde(alias = "setPollingSource", alias = "setpollingsource")]
    SetPollingSource(#[serde_as(as = "SetPollingSourceDef")] SetPollingSource),
    #[serde(alias = "setTransform", alias = "settransform")]
    SetTransform(#[serde_as(as = "SetTransformDef")] SetTransform),
    #[serde(alias = "setVocab", alias = "setvocab")]
    SetVocab(#[serde_as(as = "SetVocabDef")] SetVocab),
    #[serde(alias = "setAttachments", alias = "setattachments")]
    SetAttachments(#[serde_as(as = "SetAttachmentsDef")] SetAttachments),
    #[serde(alias = "setInfo", alias = "setinfo")]
    SetInfo(#[serde_as(as = "SetInfoDef")] SetInfo),
    #[serde(alias = "setLicense", alias = "setlicense")]
    SetLicense(#[serde_as(as = "SetLicenseDef")] SetLicense),
    #[serde(alias = "setDataSchema", alias = "setdataschema")]
    SetDataSchema(#[serde_as(as = "SetDataSchemaDef")] SetDataSchema),
    #[serde(alias = "addPushSource", alias = "addpushsource")]
    AddPushSource(#[serde_as(as = "AddPushSourceDef")] AddPushSource),
    #[serde(alias = "disablePushSource", alias = "disablepushsource")]
    DisablePushSource(#[serde_as(as = "DisablePushSourceDef")] DisablePushSource),
    #[serde(alias = "disablePollingSource", alias = "disablepollingsource")]
    DisablePollingSource(#[serde_as(as = "DisablePollingSourceDef")] DisablePollingSource),
}

implement_serde_as!(MetadataEvent, MetadataEventDef, "MetadataEventDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttQos
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqttqos-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MqttQos")]
#[serde(deny_unknown_fields)]
pub enum MqttQosDef {
    #[serde(alias = "atMostOnce", alias = "atmostonce")]
    AtMostOnce,
    #[serde(alias = "atLeastOnce", alias = "atleastonce")]
    AtLeastOnce,
    #[serde(alias = "exactlyOnce", alias = "exactlyonce")]
    ExactlyOnce,
}

implement_serde_as!(MqttQos, MqttQosDef, "MqttQosDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MqttTopicSubscription
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mqtttopicsubscription-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "MqttTopicSubscription")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MqttTopicSubscriptionDef {
    pub path: String,
    #[serde_as(as = "Option<MqttQosDef>")]
    #[serde(default)]
    pub qos: Option<MqttQos>,
}

implement_serde_as!(
    MqttTopicSubscription,
    MqttTopicSubscriptionDef,
    "MqttTopicSubscriptionDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "OffsetInterval")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OffsetIntervalDef {
    pub start: u64,
    pub end: u64,
}

implement_serde_as!(OffsetInterval, OffsetIntervalDef, "OffsetIntervalDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "PrepStep")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum PrepStepDef {
    #[serde(alias = "decompress")]
    Decompress(#[serde_as(as = "PrepStepDecompressDef")] PrepStepDecompress),
    #[serde(alias = "pipe")]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryRequest")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RawQueryRequestDef {
    pub input_data_paths: Vec<PathBuf>,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
    pub output_data_path: PathBuf,
}

implement_serde_as!(RawQueryRequest, RawQueryRequestDef, "RawQueryRequestDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RawQueryResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#rawqueryresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryResponse")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum RawQueryResponseDef {
    #[serde(alias = "progress")]
    Progress(#[serde_as(as = "RawQueryResponseProgressDef")] RawQueryResponseProgress),
    #[serde(alias = "success")]
    Success(#[serde_as(as = "RawQueryResponseSuccessDef")] RawQueryResponseSuccess),
    #[serde(alias = "invalidQuery", alias = "invalidquery")]
    InvalidQuery(#[serde_as(as = "RawQueryResponseInvalidQueryDef")] RawQueryResponseInvalidQuery),
    #[serde(alias = "internalError", alias = "internalerror")]
    InternalError(
        #[serde_as(as = "RawQueryResponseInternalErrorDef")] RawQueryResponseInternalError,
    ),
}

implement_serde_as!(RawQueryResponse, RawQueryResponseDef, "RawQueryResponseDef");
implement_serde_as!(
    RawQueryResponseProgress,
    RawQueryResponseProgressDef,
    "RawQueryResponseProgressDef"
);
implement_serde_as!(
    RawQueryResponseSuccess,
    RawQueryResponseSuccessDef,
    "RawQueryResponseSuccessDef"
);
implement_serde_as!(
    RawQueryResponseInvalidQuery,
    RawQueryResponseInvalidQueryDef,
    "RawQueryResponseInvalidQueryDef"
);
implement_serde_as!(
    RawQueryResponseInternalError,
    RawQueryResponseInternalErrorDef,
    "RawQueryResponseInternalErrorDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryResponseProgress")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RawQueryResponseProgressDef {}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryResponseSuccess")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RawQueryResponseSuccessDef {
    pub num_records: u64,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryResponseInvalidQuery")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RawQueryResponseInvalidQueryDef {
    pub message: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "RawQueryResponseInternalError")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RawQueryResponseInternalErrorDef {
    pub message: String,
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStep")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum ReadStepDef {
    #[serde(alias = "csv")]
    Csv(#[serde_as(as = "ReadStepCsvDef")] ReadStepCsv),
    #[serde(alias = "geoJson", alias = "geojson")]
    GeoJson(#[serde_as(as = "ReadStepGeoJsonDef")] ReadStepGeoJson),
    #[serde(alias = "esriShapefile", alias = "esrishapefile")]
    EsriShapefile(#[serde_as(as = "ReadStepEsriShapefileDef")] ReadStepEsriShapefile),
    #[serde(alias = "parquet")]
    Parquet(#[serde_as(as = "ReadStepParquetDef")] ReadStepParquet),
    #[serde(alias = "json")]
    Json(#[serde_as(as = "ReadStepJsonDef")] ReadStepJson),
    #[serde(alias = "ndJson", alias = "ndjson")]
    NdJson(#[serde_as(as = "ReadStepNdJsonDef")] ReadStepNdJson),
    #[serde(alias = "ndGeoJson", alias = "ndgeojson")]
    NdGeoJson(#[serde_as(as = "ReadStepNdGeoJsonDef")] ReadStepNdGeoJson),
}

implement_serde_as!(ReadStep, ReadStepDef, "ReadStepDef");
implement_serde_as!(ReadStepCsv, ReadStepCsvDef, "ReadStepCsvDef");
implement_serde_as!(ReadStepJson, ReadStepJsonDef, "ReadStepJsonDef");
implement_serde_as!(ReadStepNdJson, ReadStepNdJsonDef, "ReadStepNdJsonDef");
implement_serde_as!(ReadStepGeoJson, ReadStepGeoJsonDef, "ReadStepGeoJsonDef");
implement_serde_as!(
    ReadStepNdGeoJson,
    ReadStepNdGeoJsonDef,
    "ReadStepNdGeoJsonDef"
);
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
    pub header: Option<bool>,
    pub infer_schema: Option<bool>,
    pub null_value: Option<String>,
    pub date_format: Option<String>,
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

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepJson")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepJsonDef {
    pub sub_path: Option<String>,
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
    pub timestamp_format: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepNdJson")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepNdJsonDef {
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
    pub timestamp_format: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "ReadStepNdGeoJson")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadStepNdGeoJsonDef {
    pub schema: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RequestHeader
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#requestheader-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Seed")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SeedDef {
    pub dataset_id: DatasetID,
    #[serde_as(as = "DatasetKindDef")]
    pub dataset_kind: DatasetKind,
}

implement_serde_as!(Seed, SeedDef, "SeedDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetAttachments
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setattachments-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetDataSchema
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setdataschema-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetDataSchema")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetDataSchemaDef {
    #[serde(with = "base64")]
    pub schema: Vec<u8>,
}

implement_serde_as!(SetDataSchema, SetDataSchemaDef, "SetDataSchemaDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetInfo
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setinfo-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetLicense
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setlicense-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SetVocab")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SetVocabDef {
    pub offset_column: Option<String>,
    pub operation_type_column: Option<String>,
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
}

implement_serde_as!(SetVocab, SetVocabDef, "SetVocabDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceCaching")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum SourceCachingDef {
    #[serde(alias = "forever")]
    Forever(#[serde_as(as = "SourceCachingForeverDef")] SourceCachingForever),
}

implement_serde_as!(SourceCaching, SourceCachingDef, "SourceCachingDef");
implement_serde_as!(
    SourceCachingForever,
    SourceCachingForeverDef,
    "SourceCachingForeverDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceCachingForever")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceCachingForeverDef {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceOrdering
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourceordering-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceOrdering")]
#[serde(deny_unknown_fields)]
pub enum SourceOrderingDef {
    #[serde(alias = "byEventTime", alias = "byeventtime")]
    ByEventTime,
    #[serde(alias = "byName", alias = "byname")]
    ByName,
}

implement_serde_as!(SourceOrdering, SourceOrderingDef, "SourceOrderingDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SourceState
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcestate-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceState")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceStateDef {
    pub source_name: String,
    pub kind: String,
    pub value: String,
}

implement_serde_as!(SourceState, SourceStateDef, "SourceStateDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SqlQueryStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sqlquerystep-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Transform")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum TransformDef {
    #[serde(alias = "sql")]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformInputDef {
    pub dataset_ref: DatasetRef,
    pub alias: Option<String>,
}

implement_serde_as!(TransformInput, TransformInputDef, "TransformInputDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequest-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformRequest")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformRequestDef {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
    #[serde_as(as = "Vec<TransformRequestInputDef>")]
    pub query_inputs: Vec<TransformRequestInput>,
    pub next_offset: u64,
    pub prev_checkpoint_path: Option<PathBuf>,
    pub new_checkpoint_path: PathBuf,
    pub new_data_path: PathBuf,
}

implement_serde_as!(TransformRequest, TransformRequestDef, "TransformRequestDef");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformRequestInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformrequestinput-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformRequestInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformRequestInputDef {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
    pub query_alias: String,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    #[serde_as(as = "Option<OffsetIntervalDef>")]
    #[serde(default)]
    pub offset_interval: Option<OffsetInterval>,
    pub data_paths: Vec<PathBuf>,
    pub schema_file: PathBuf,
    #[serde_as(as = "Vec<WatermarkDef>")]
    pub explicit_watermarks: Vec<Watermark>,
}

implement_serde_as!(
    TransformRequestInput,
    TransformRequestInputDef,
    "TransformRequestInputDef"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TransformResponse
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transformresponse-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformResponse")]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum TransformResponseDef {
    #[serde(alias = "progress")]
    Progress(#[serde_as(as = "TransformResponseProgressDef")] TransformResponseProgress),
    #[serde(alias = "success")]
    Success(#[serde_as(as = "TransformResponseSuccessDef")] TransformResponseSuccess),
    #[serde(alias = "invalidQuery", alias = "invalidquery")]
    InvalidQuery(
        #[serde_as(as = "TransformResponseInvalidQueryDef")] TransformResponseInvalidQuery,
    ),
    #[serde(alias = "internalError", alias = "internalerror")]
    InternalError(
        #[serde_as(as = "TransformResponseInternalErrorDef")] TransformResponseInternalError,
    ),
}

implement_serde_as!(
    TransformResponse,
    TransformResponseDef,
    "TransformResponseDef"
);
implement_serde_as!(
    TransformResponseProgress,
    TransformResponseProgressDef,
    "TransformResponseProgressDef"
);
implement_serde_as!(
    TransformResponseSuccess,
    TransformResponseSuccessDef,
    "TransformResponseSuccessDef"
);
implement_serde_as!(
    TransformResponseInvalidQuery,
    TransformResponseInvalidQueryDef,
    "TransformResponseInvalidQueryDef"
);
implement_serde_as!(
    TransformResponseInternalError,
    TransformResponseInternalErrorDef,
    "TransformResponseInternalErrorDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformResponseProgress")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformResponseProgressDef {}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformResponseSuccess")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformResponseSuccessDef {
    #[serde_as(as = "Option<OffsetIntervalDef>")]
    #[serde(default)]
    pub new_offset_interval: Option<OffsetInterval>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub new_watermark: Option<DateTime<Utc>>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformResponseInvalidQuery")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformResponseInvalidQueryDef {
    pub message: String,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "TransformResponseInternalError")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransformResponseInternalErrorDef {
    pub message: String,
    pub backtrace: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
