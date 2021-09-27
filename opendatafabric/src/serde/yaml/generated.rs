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

////////////////////////////////////////////////////////////////////////////////

macro_rules! implement_serde_as {
    ($dto:ty, $impl:ty, $impl_name:literal) => {
        impl ::serde_with::SerializeAs<$dto> for $impl {
            fn serialize_as<S>(source: &$dto, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                #[derive(Serialize)]
                struct Helper<'a>(#[serde(with = $impl_name)] &'a $dto);
                Helper(source).serialize(serializer)
            }
        }

        impl<'de> serde_with::DeserializeAs<'de, $dto> for $impl {
            fn deserialize_as<D>(deserializer: D) -> Result<$dto, D::Error>
            where
                D: Deserializer<'de>,
            {
                #[derive(Deserialize)]
                struct Helper(#[serde(with = $impl_name)] $dto);
                let helper = Helper::deserialize(deserializer)?;
                let Helper(v) = helper;
                Ok(v)
            }
        }
    };
}

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
    pub hash: Sha3_256,
    pub interval: TimeInterval,
    pub num_records: i64,
}

implement_serde_as!(DataSlice, DataSliceDef, "DataSliceDef");

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
    pub id: DatasetIDBuf,
    #[serde_as(as = "DatasetSourceDef")]
    pub source: DatasetSource,
    #[serde_as(as = "Option<DatasetVocabularyDef>")]
    #[serde(default)]
    pub vocab: Option<DatasetVocabulary>,
}

implement_serde_as!(DatasetSnapshot, DatasetSnapshotDef, "DatasetSnapshotDef");

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum DatasetSourceDef {
    #[serde(rename_all = "camelCase")]
    Root(#[serde_as(as = "DatasetSourceRootDef")] DatasetSourceRoot),
    #[serde(rename_all = "camelCase")]
    Derivative(#[serde_as(as = "DatasetSourceDerivativeDef")] DatasetSourceDerivative),
}

implement_serde_as!(DatasetSource, DatasetSourceDef, "DatasetSourceDef");
implement_serde_as!(
    DatasetSourceRoot,
    DatasetSourceRootDef,
    "DatasetSourceRootDef"
);
implement_serde_as!(
    DatasetSourceDerivative,
    DatasetSourceDerivativeDef,
    "DatasetSourceDerivativeDef"
);

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSourceRoot")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSourceRootDef {
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

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSourceDerivative")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSourceDerivativeDef {
    pub inputs: Vec<DatasetIDBuf>,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
}

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
}

implement_serde_as!(
    DatasetVocabulary,
    DatasetVocabularyDef,
    "DatasetVocabularyDef"
);

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
    pub dataset_id: DatasetIDBuf,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    #[serde_as(as = "TransformDef")]
    pub transform: Transform,
    #[serde_as(as = "Vec<QueryInputDef>")]
    pub inputs: Vec<QueryInput>,
    pub prev_checkpoint_dir: Option<String>,
    pub new_checkpoint_dir: String,
    pub out_data_path: String,
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
    #[serde_as(as = "MetadataBlockDef")]
    pub metadata_block: MetadataBlock,
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
}

implement_serde_as!(FetchStep, FetchStepDef, "FetchStepDef");
implement_serde_as!(FetchStepUrl, FetchStepUrlDef, "FetchStepUrlDef");
implement_serde_as!(
    FetchStepFilesGlob,
    FetchStepFilesGlobDef,
    "FetchStepFilesGlobDef"
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceOrdering")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum SourceOrderingDef {
    ByEventTime,
    ByName,
}

implement_serde_as!(SourceOrdering, SourceOrderingDef, "SourceOrderingDef");

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
    pub block_hash: Sha3_256,
    pub prev_block_hash: Option<Sha3_256>,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde_as(as = "Option<DataSliceDef>")]
    #[serde(default)]
    pub output_slice: Option<DataSlice>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub output_watermark: Option<DateTime<Utc>>,
    #[serde_as(as = "Option<Vec<DataSliceDef>>")]
    #[serde(default)]
    pub input_slices: Option<Vec<DataSlice>>,
    #[serde_as(as = "Option<DatasetSourceDef>")]
    #[serde(default)]
    pub source: Option<DatasetSource>,
    #[serde_as(as = "Option<DatasetVocabularyDef>")]
    #[serde(default)]
    pub vocab: Option<DatasetVocabulary>,
}

implement_serde_as!(MetadataBlock, MetadataBlockDef, "MetadataBlockDef");

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
// QueryInput
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#queryinput-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "QueryInput")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryInputDef {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetIDBuf,
    #[serde_as(as = "DatasetVocabularyDef")]
    pub vocab: DatasetVocabulary,
    pub interval: TimeInterval,
    pub data_paths: Vec<String>,
    pub schema_file: String,
    #[serde_as(as = "Vec<WatermarkDef>")]
    pub explicit_watermarks: Vec<Watermark>,
}

implement_serde_as!(QueryInput, QueryInputDef, "QueryInputDef");

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
    pub id: String,
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
