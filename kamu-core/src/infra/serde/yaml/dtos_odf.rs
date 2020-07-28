////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::domain::DatasetIDBuf;
use crate::domain::TimeInterval;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use serde_yaml::Value;
use std::collections::BTreeMap;

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetSource {
  #[serde(rename_all = "camelCase")]
  Root(DatasetSourceRoot),
  #[serde(rename_all = "camelCase")]
  Derivative(DatasetSourceDerivative),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSourceRoot {
  pub fetch: FetchStep,
  pub prepare: Option<Vec<PrepStep>>,
  pub read: ReadStep,
  pub preprocess: Option<Transform>,
  pub merge: MergeStrategy,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSourceDerivative {
  pub inputs: Vec<DatasetIDBuf>,
  pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetVocabulary {
  pub system_time_column: Option<String>,
  pub event_time_column: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceCaching {
  #[serde(rename_all = "camelCase")]
  Forever,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSnapshot {
  pub id: DatasetIDBuf,
  pub source: DatasetSource,
  pub vocab: Option<DatasetVocabulary>,
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSlice {
  pub hash: String,
  pub interval: TimeInterval,
  pub num_records: i64,
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
  #[serde(rename_all = "camelCase")]
  Append,
  #[serde(rename_all = "camelCase")]
  Ledger(MergeStrategyLedger),
  #[serde(rename_all = "camelCase")]
  Snapshot(MergeStrategySnapshot),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeStrategyLedger {
  pub primary_key: Vec<String>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBlock {
  pub block_hash: String,
  pub prev_block_hash: String,
  #[serde(with = "datetime_rfc3339")]
  pub system_time: DateTime<Utc>,
  pub output_slice: Option<DataSlice>,
  #[serde(default, with = "datetime_rfc3339_opt")]
  pub output_watermark: Option<DateTime<Utc>>,
  pub input_slices: Option<Vec<DataSlice>>,
  pub source: Option<DatasetSource>,
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadStep {
  #[serde(rename_all = "camelCase")]
  Csv(ReadStepCsv),
  #[serde(rename_all = "camelCase")]
  JsonLines(ReadStepJsonLines),
  #[serde(rename_all = "camelCase")]
  GeoJson(ReadStepGeoJson),
  #[serde(rename_all = "camelCase")]
  EsriShapefile(ReadStepEsriShapefile),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStepJsonLines {
  pub schema: Option<Vec<String>>,
  pub date_format: Option<String>,
  pub encoding: Option<String>,
  pub multi_line: Option<bool>,
  pub primitives_as_string: Option<bool>,
  pub timestamp_format: Option<String>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStepGeoJson {
  pub schema: Option<Vec<String>>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStepEsriShapefile {
  pub schema: Option<Vec<String>>,
  pub sub_path: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transform {
  pub engine: String,
  #[serde(flatten)]
  pub additional_properties: BTreeMap<String, Value>,
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchStep {
  #[serde(rename_all = "camelCase")]
  Url(FetchStepUrl),
  #[serde(rename_all = "camelCase")]
  FilesGlob(FetchStepFilesGlob),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchStepUrl {
  pub url: String,
  pub event_time: Option<EventTimeSource>,
  pub cache: Option<SourceCaching>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchStepFilesGlob {
  pub path: String,
  pub event_time: Option<EventTimeSource>,
  pub cache: Option<SourceCaching>,
  pub order: Option<SourceOrdering>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceOrdering {
  ByEventTime,
  ByName,
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrepStep {
  #[serde(rename_all = "camelCase")]
  Decompress(PrepStepDecompress),
  #[serde(rename_all = "camelCase")]
  Pipe(PrepStepPipe),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepStepDecompress {
  pub format: CompressionFormat,
  pub sub_path: Option<String>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepStepPipe {
  pub command: Vec<String>,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionFormat {
  Gzip,
  Zip,
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventTimeSource {
  #[serde(rename_all = "camelCase")]
  FromPath(EventTimeSourceFromPath),
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTimeSourceFromPath {
  pub pattern: String,
  pub timestamp_format: Option<String>,
}
