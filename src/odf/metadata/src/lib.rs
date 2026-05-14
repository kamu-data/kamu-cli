// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]

pub mod formats;

pub mod auth;
pub mod config;
pub mod data;
pub mod dataset;
pub mod dtos;
pub mod engine;
pub mod errors;
pub mod resource;
pub mod serde;
pub mod storage;

// Re-exports
pub use dtos::*;
pub use enum_variants::{EnumWithVariants, VariantOf};
pub use formats::multiformats::stack_string::{AsStackString, ToStackString};
pub use formats::*;

#[cfg(any(feature = "testing", test))]
pub mod testing;

// TODO: LEGACY
// These re-exports are to support legacy flat type structure. You should not be
// adding more types to this list. New code should import types as
// `odf::{context}::{type}`.
pub use auth::{AccountID, AccountName};
pub use data as schema;
pub use data::{
    ArrowBufferEncoding,
    DataField,
    DataSchema,
    DataSchemaCmp,
    DataSchemaCmpOptions,
    DataSchemaDiff,
    DataSchemaDiffItem,
    DataTypeDiffItem,
    ExtraAttributes,
    InvalidOperationType,
    OperationType,
    ToArrowSettings,
};
pub use dataset::{
    AddData,
    AsTypedBlock,
    AttachmentEmbedded,
    Attachments,
    AttachmentsEmbedded,
    Checkpoint,
    DataSlice,
    DatasetAlias,
    DatasetAliasRemote,
    DatasetHandle,
    DatasetHandleRemote,
    DatasetID,
    DatasetKind,
    DatasetName,
    DatasetPushTarget,
    DatasetRef,
    DatasetRefAny,
    DatasetRefAnyPattern,
    DatasetRefPattern,
    DatasetRefRemote,
    DatasetVocabulary,
    ExecuteTransform,
    ExecuteTransformInput,
    IntoDataStreamBlock,
    IntoDataStreamEvent,
    MetadataBlock,
    MetadataBlockDataStream,
    MetadataBlockDataStreamRef,
    MetadataBlockTyped,
    MetadataEvent,
    MetadataEventDataStream,
    MetadataEventTypeFlags,
    OffsetInterval,
    Seed,
    SetAttachments,
    SetDataSchema,
    SetInfo,
    SetLicense,
    SetTransform,
    SetVocab,
    SqlQueryStep,
    TemporalTable,
    Transform,
    TransformInput,
    TransformSql,
    Watermark,
};
pub use ed25519_dalek as ed25519;
pub use engine::{
    RawQueryRequest,
    RawQueryResponse,
    RawQueryResponseInternalError,
    RawQueryResponseSuccess,
    TransformRequest,
    TransformRequestInput,
    TransformResponse,
    TransformResponseInternalError,
    TransformResponseInvalidQuery,
    TransformResponseProgress,
    TransformResponseSuccess,
};
pub use errors::AccessError;
pub use formats::{DidOdf, DidPkh};
pub use legacy::{
    AddPushSource,
    DatasetSnapshot,
    DisablePollingSource,
    DisablePushSource,
    FetchStep,
    FetchStepContainer,
    FetchStepEthereumLogs,
    FetchStepFilesGlob,
    FetchStepMqtt,
    FetchStepUrl,
    Manifest,
    SetPollingSource,
};
pub use source::{
    CompressionFormat,
    EnvVar,
    EventTimeSource,
    EventTimeSourceFromMetadata,
    EventTimeSourceFromPath,
    EventTimeSourceFromSystemTime,
    MergeStrategy,
    MergeStrategyAppend,
    MergeStrategyChangelogStream,
    MergeStrategyLedger,
    MergeStrategySnapshot,
    MergeStrategyUpsertStream,
    MqttQos,
    MqttTopicSubscription,
    PrepStep,
    PrepStepDecompress,
    PrepStepPipe,
    ReadStep,
    ReadStepCsv,
    ReadStepEsriShapefile,
    ReadStepGeoJson,
    ReadStepJson,
    ReadStepNdGeoJson,
    ReadStepNdJson,
    ReadStepParquet,
    RequestHeader,
    SourceCaching,
    SourceCachingForever,
    SourceOrdering,
    SourceState,
};
pub use storage::RepoName;
