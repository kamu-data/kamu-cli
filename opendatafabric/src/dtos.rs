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

use std::path::PathBuf;

use super::{DatasetID, DatasetName, Multihash};
use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////
// AddData
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#adddata-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AddData {
    pub output_data: DataSlice,
    pub output_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////
// BlockInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#blockinterval-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlockInterval {
    pub start: Multihash,
    pub end: Multihash,
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataSlice {
    pub logical_hash: Multihash,
    pub physical_hash: Multihash,
    pub interval: OffsetInterval,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSnapshot {
    pub name: DatasetName,
    pub kind: DatasetKind,
    pub metadata: Vec<MetadataEvent>,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetVocabulary {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EventTimeSourceFromPath {
    pub pattern: String,
    pub timestamp_format: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQuery
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequery-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQuery {
    pub input_slices: Vec<InputSlice>,
    pub output_data: Option<DataSlice>,
    pub output_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryinput-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryInput {
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    pub vocab: DatasetVocabulary,
    pub data_interval: Option<OffsetInterval>,
    pub data_paths: Vec<PathBuf>,
    pub schema_file: PathBuf,
    pub explicit_watermarks: Vec<Watermark>,
}

////////////////////////////////////////////////////////////////////////////////
// ExecuteQueryRequest
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#executequeryrequest-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryRequest {
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    pub system_time: DateTime<Utc>,
    pub offset: i64,
    pub vocab: DatasetVocabulary,
    pub transform: Transform,
    pub inputs: Vec<ExecuteQueryInput>,
    pub prev_checkpoint_dir: Option<PathBuf>,
    pub new_checkpoint_dir: PathBuf,
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
    pub data_interval: Option<OffsetInterval>,
    pub output_watermark: Option<DateTime<Utc>>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryResponseInvalidQuery {
    pub message: String,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ExecuteQueryResponseInternalError {
    pub message: String,
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
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepUrl {
    pub url: String,
    pub event_time: Option<EventTimeSource>,
    pub cache: Option<SourceCaching>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchStepFilesGlob {
    pub path: String,
    pub event_time: Option<EventTimeSource>,
    pub cache: Option<SourceCaching>,
    pub order: Option<SourceOrdering>,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InputSlice {
    pub dataset_id: DatasetID,
    pub block_interval: Option<BlockInterval>,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategyLedger {
    pub primary_key: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MergeStrategySnapshot {
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MetadataBlock {
    pub system_time: DateTime<Utc>,
    pub prev_block_hash: Option<Multihash>,
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
}

////////////////////////////////////////////////////////////////////////////////
// OffsetInterval
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#offsetinterval-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OffsetInterval {
    pub start: i64,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepDecompress {
    pub format: CompressionFormat,
    pub sub_path: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PrepStepPipe {
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepCsv {
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepJsonLines {
    pub schema: Option<Vec<String>>,
    pub date_format: Option<String>,
    pub encoding: Option<String>,
    pub multi_line: Option<bool>,
    pub primitives_as_string: Option<bool>,
    pub timestamp_format: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepGeoJson {
    pub schema: Option<Vec<String>>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadStepEsriShapefile {
    pub schema: Option<Vec<String>>,
    pub sub_path: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// Seed
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#seed-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Seed {
    pub dataset_id: DatasetID,
    pub dataset_kind: DatasetKind,
}

////////////////////////////////////////////////////////////////////////////////
// SetPollingSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setpollingsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetPollingSource {
    pub fetch: FetchStep,
    pub prepare: Option<Vec<PrepStep>>,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

////////////////////////////////////////////////////////////////////////////////
// SetTransform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#settransform-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetTransform {
    pub inputs: Vec<TransformInput>,
    pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////
// SetVocab
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setvocab-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetVocab {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
    pub offset_column: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// SetWatermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#setwatermark-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SetWatermark {
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SqlQueryStep {
    pub alias: Option<String>,
    pub query: String,
}

////////////////////////////////////////////////////////////////////////////////
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TemporalTable {
    pub name: String,
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformSql {
    pub engine: String,
    pub version: Option<String>,
    pub query: Option<String>,
    pub queries: Option<Vec<SqlQueryStep>>,
    pub temporal_tables: Option<Vec<TemporalTable>>,
}

////////////////////////////////////////////////////////////////////////////////
// TransformInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transforminput-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TransformInput {
    pub id: Option<DatasetID>,
    pub name: DatasetName,
}

////////////////////////////////////////////////////////////////////////////////
// Watermark
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#watermark-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Watermark {
    pub system_time: DateTime<Utc>,
    pub event_time: DateTime<Utc>,
}
