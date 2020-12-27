////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::{DatasetIDBuf, Sha3_256, TimeInterval};
use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DatasetSource {
    Root(DatasetSourceRoot),
    Derivative(DatasetSourceDerivative),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSourceRoot {
    pub fetch: FetchStep,
    pub prepare: Option<Vec<PrepStep>>,
    pub read: ReadStep,
    pub preprocess: Option<Transform>,
    pub merge: MergeStrategy,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSourceDerivative {
    pub inputs: Vec<DatasetIDBuf>,
    pub transform: Transform,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetVocabulary
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetvocabulary-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetVocabulary {
    pub system_time_column: Option<String>,
    pub event_time_column: Option<String>,
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
// TemporalTable
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#temporaltable-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TemporalTable {
    pub id: String,
    pub primary_key: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////
// DatasetSnapshot
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsnapshot-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DatasetSnapshot {
    pub id: DatasetIDBuf,
    pub source: DatasetSource,
    pub vocab: Option<DatasetVocabulary>,
}

////////////////////////////////////////////////////////////////////////////////
// DataSlice
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataslice-schema
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DataSlice {
    pub hash: Sha3_256,
    pub interval: TimeInterval,
    pub num_records: i64,
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
    pub block_hash: Sha3_256,
    pub prev_block_hash: Option<Sha3_256>,
    pub system_time: DateTime<Utc>,
    pub output_slice: Option<DataSlice>,
    pub output_watermark: Option<DateTime<Utc>>,
    pub input_slices: Option<Vec<DataSlice>>,
    pub source: Option<DatasetSource>,
    pub vocab: Option<DatasetVocabulary>,
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
