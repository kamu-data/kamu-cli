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

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetSource {
  #[serde(rename_all = "camelCase")]
  Root {
    fetch: FetchStep,
    prepare: Option<Vec<PrepStep>>,
    read: ReadStep,
    preprocess: Option<Transform>,
    merge: MergeStrategy,
  },
  #[serde(rename_all = "camelCase")]
  Derivative {
    inputs: Vec<DatasetIDBuf>,
    transform: Transform,
  },
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceCaching {
  #[serde(rename_all = "camelCase")]
  Forever,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeStrategy {
  #[serde(rename_all = "camelCase")]
  Append,
  #[serde(rename_all = "camelCase")]
  Ledger { primary_key: Vec<String> },
  #[serde(rename_all = "camelCase")]
  Snapshot {
    primary_key: Vec<String>,
    compare_columns: Option<Vec<String>>,
    observation_column: Option<String>,
    obsv_added: Option<String>,
    obsv_changed: Option<String>,
    obsv_removed: Option<String>,
  },
}

////////////////////////////////////////////////////////////////////////////////
// MetadataBlock
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#metadatablock-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadStep {
  #[serde(rename_all = "camelCase")]
  Csv {
    schema: Option<Vec<String>>,
    separator: Option<String>,
    encoding: Option<String>,
    quote: Option<String>,
    escape: Option<String>,
    comment: Option<String>,
    header: Option<bool>,
    enforce_schema: Option<bool>,
    infer_schema: Option<bool>,
    ignore_leading_white_space: Option<bool>,
    ignore_trailing_white_space: Option<bool>,
    null_value: Option<String>,
    empty_value: Option<String>,
    nan_value: Option<String>,
    positive_inf: Option<String>,
    negative_inf: Option<String>,
    date_format: Option<String>,
    timestamp_format: Option<String>,
    multi_line: Option<bool>,
  },
  #[serde(rename_all = "camelCase")]
  JsonLines {
    schema: Option<Vec<String>>,
    date_format: Option<String>,
    encoding: Option<String>,
    multi_line: Option<bool>,
    primitives_as_string: Option<bool>,
    timestamp_format: Option<String>,
  },
  #[serde(rename_all = "camelCase")]
  GeoJson { schema: Option<Vec<String>> },
  #[serde(rename_all = "camelCase")]
  EsriShapefile {
    schema: Option<Vec<String>>,
    sub_path: Option<String>,
  },
}

////////////////////////////////////////////////////////////////////////////////
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transform {
  pub engine: String,
}

////////////////////////////////////////////////////////////////////////////////
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchStep {
  #[serde(rename_all = "camelCase")]
  Url {
    url: String,
    event_time: Option<EventTimeSource>,
    cache: Option<SourceCaching>,
  },
  #[serde(rename_all = "camelCase")]
  FilesGlob {
    path: String,
    event_time: Option<EventTimeSource>,
    cache: Option<SourceCaching>,
    order: Option<SourceOrdering>,
  },
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrepStep {
  #[serde(rename_all = "camelCase")]
  Decompress {
    format: CompressionFormat,
    sub_path: Option<String>,
  },
  #[serde(rename_all = "camelCase")]
  Pipe { command: Vec<String> },
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventTimeSource {
  #[serde(rename_all = "camelCase")]
  FromPath {
    pattern: String,
    timestamp_format: Option<String>,
  },
}
