////////////////////////////////////////////////////////////////////////////////
// WARNING: This file is auto-generated from Open Data Fabric Schemas
// See: http://opendatafabric.org/
////////////////////////////////////////////////////////////////////////////////

use super::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::*;
use ::chrono::{DateTime, Utc};
use ::serde::{Deserialize, Deserializer, Serialize, Serializer};
use ::serde_with::serde_as;
use ::serde_with::skip_serializing_none;

////////////////////////////////////////////////////////////////////////////////
// DatasetSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#datasetsource-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "DatasetSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum DatasetSourceDef {
    #[serde(rename_all = "camelCase")]
    Root(#[serde_as(as = "DatasetSourceRootDef")] DatasetSourceRoot),
    #[serde(rename_all = "camelCase")]
    Derivative(#[serde_as(as = "DatasetSourceDerivativeDef")] DatasetSourceDerivative),
}

impl serde_with::SerializeAs<DatasetSource> for DatasetSourceDef {
    fn serialize_as<S>(source: &DatasetSource, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "DatasetSourceDef")] &'a DatasetSource);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DatasetSource> for DatasetSourceDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DatasetSource, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DatasetSourceDef")] DatasetSource);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<DatasetSourceRoot> for DatasetSourceRootDef {
    fn serialize_as<S>(source: &DatasetSourceRoot, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "DatasetSourceRootDef")] &'a DatasetSourceRoot);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DatasetSourceRoot> for DatasetSourceRootDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DatasetSourceRoot, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DatasetSourceRootDef")] DatasetSourceRoot);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<DatasetSourceDerivative> for DatasetSourceDerivativeDef {
    fn serialize_as<S>(source: &DatasetSourceDerivative, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(
            #[serde(with = "DatasetSourceDerivativeDef")] &'a DatasetSourceDerivative,
        );
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DatasetSourceDerivative> for DatasetSourceDerivativeDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DatasetSourceDerivative, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DatasetSourceDerivativeDef")] DatasetSourceDerivative);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

impl serde_with::SerializeAs<DatasetVocabulary> for DatasetVocabularyDef {
    fn serialize_as<S>(source: &DatasetVocabulary, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "DatasetVocabularyDef")] &'a DatasetVocabulary);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DatasetVocabulary> for DatasetVocabularyDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DatasetVocabulary, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DatasetVocabularyDef")] DatasetVocabulary);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

////////////////////////////////////////////////////////////////////////////////
// SourceCaching
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#sourcecaching-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceCaching")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum SourceCachingDef {
    #[serde(rename_all = "camelCase")]
    Forever,
}

impl serde_with::SerializeAs<SourceCaching> for SourceCachingDef {
    fn serialize_as<S>(source: &SourceCaching, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "SourceCachingDef")] &'a SourceCaching);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, SourceCaching> for SourceCachingDef {
    fn deserialize_as<D>(deserializer: D) -> Result<SourceCaching, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "SourceCachingDef")] SourceCaching);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

impl serde_with::SerializeAs<TemporalTable> for TemporalTableDef {
    fn serialize_as<S>(source: &TemporalTable, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "TemporalTableDef")] &'a TemporalTable);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, TemporalTable> for TemporalTableDef {
    fn deserialize_as<D>(deserializer: D) -> Result<TemporalTable, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "TemporalTableDef")] TemporalTable);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

impl serde_with::SerializeAs<DatasetSnapshot> for DatasetSnapshotDef {
    fn serialize_as<S>(source: &DatasetSnapshot, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "DatasetSnapshotDef")] &'a DatasetSnapshot);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DatasetSnapshot> for DatasetSnapshotDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DatasetSnapshot, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DatasetSnapshotDef")] DatasetSnapshot);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
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

impl serde_with::SerializeAs<DataSlice> for DataSliceDef {
    fn serialize_as<S>(source: &DataSlice, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "DataSliceDef")] &'a DataSlice);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, DataSlice> for DataSliceDef {
    fn deserialize_as<D>(deserializer: D) -> Result<DataSlice, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "DataSliceDef")] DataSlice);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

impl serde_with::SerializeAs<SqlQueryStep> for SqlQueryStepDef {
    fn serialize_as<S>(source: &SqlQueryStep, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "SqlQueryStepDef")] &'a SqlQueryStep);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, SqlQueryStep> for SqlQueryStepDef {
    fn deserialize_as<D>(deserializer: D) -> Result<SqlQueryStep, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "SqlQueryStepDef")] SqlQueryStep);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

////////////////////////////////////////////////////////////////////////////////
// MergeStrategy
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#mergestrategy-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
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

impl serde_with::SerializeAs<MergeStrategy> for MergeStrategyDef {
    fn serialize_as<S>(source: &MergeStrategy, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "MergeStrategyDef")] &'a MergeStrategy);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, MergeStrategy> for MergeStrategyDef {
    fn deserialize_as<D>(deserializer: D) -> Result<MergeStrategy, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "MergeStrategyDef")] MergeStrategy);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<MergeStrategyLedger> for MergeStrategyLedgerDef {
    fn serialize_as<S>(source: &MergeStrategyLedger, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "MergeStrategyLedgerDef")] &'a MergeStrategyLedger);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, MergeStrategyLedger> for MergeStrategyLedgerDef {
    fn deserialize_as<D>(deserializer: D) -> Result<MergeStrategyLedger, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "MergeStrategyLedgerDef")] MergeStrategyLedger);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<MergeStrategySnapshot> for MergeStrategySnapshotDef {
    fn serialize_as<S>(source: &MergeStrategySnapshot, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "MergeStrategySnapshotDef")] &'a MergeStrategySnapshot);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, MergeStrategySnapshot> for MergeStrategySnapshotDef {
    fn deserialize_as<D>(deserializer: D) -> Result<MergeStrategySnapshot, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "MergeStrategySnapshotDef")] MergeStrategySnapshot);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

impl serde_with::SerializeAs<MetadataBlock> for MetadataBlockDef {
    fn serialize_as<S>(source: &MetadataBlock, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "MetadataBlockDef")] &'a MetadataBlock);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, MetadataBlock> for MetadataBlockDef {
    fn deserialize_as<D>(deserializer: D) -> Result<MetadataBlock, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "MetadataBlockDef")] MetadataBlock);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ReadStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#readstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
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

impl serde_with::SerializeAs<ReadStep> for ReadStepDef {
    fn serialize_as<S>(source: &ReadStep, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "ReadStepDef")] &'a ReadStep);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, ReadStep> for ReadStepDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ReadStep, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "ReadStepDef")] ReadStep);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<ReadStepCsv> for ReadStepCsvDef {
    fn serialize_as<S>(source: &ReadStepCsv, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "ReadStepCsvDef")] &'a ReadStepCsv);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, ReadStepCsv> for ReadStepCsvDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ReadStepCsv, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "ReadStepCsvDef")] ReadStepCsv);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<ReadStepJsonLines> for ReadStepJsonLinesDef {
    fn serialize_as<S>(source: &ReadStepJsonLines, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "ReadStepJsonLinesDef")] &'a ReadStepJsonLines);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, ReadStepJsonLines> for ReadStepJsonLinesDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ReadStepJsonLines, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "ReadStepJsonLinesDef")] ReadStepJsonLines);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<ReadStepGeoJson> for ReadStepGeoJsonDef {
    fn serialize_as<S>(source: &ReadStepGeoJson, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "ReadStepGeoJsonDef")] &'a ReadStepGeoJson);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, ReadStepGeoJson> for ReadStepGeoJsonDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ReadStepGeoJson, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "ReadStepGeoJsonDef")] ReadStepGeoJson);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<ReadStepEsriShapefile> for ReadStepEsriShapefileDef {
    fn serialize_as<S>(source: &ReadStepEsriShapefile, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "ReadStepEsriShapefileDef")] &'a ReadStepEsriShapefile);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, ReadStepEsriShapefile> for ReadStepEsriShapefileDef {
    fn deserialize_as<D>(deserializer: D) -> Result<ReadStepEsriShapefile, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "ReadStepEsriShapefileDef")] ReadStepEsriShapefile);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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
// Transform
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#transform-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "Transform")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum TransformDef {
    #[serde(rename_all = "camelCase")]
    Sql(#[serde_as(as = "TransformSqlDef")] TransformSql),
}

impl serde_with::SerializeAs<Transform> for TransformDef {
    fn serialize_as<S>(source: &Transform, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "TransformDef")] &'a Transform);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, Transform> for TransformDef {
    fn deserialize_as<D>(deserializer: D) -> Result<Transform, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "TransformDef")] Transform);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<TransformSql> for TransformSqlDef {
    fn serialize_as<S>(source: &TransformSql, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "TransformSqlDef")] &'a TransformSql);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, TransformSql> for TransformSqlDef {
    fn deserialize_as<D>(deserializer: D) -> Result<TransformSql, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "TransformSqlDef")] TransformSql);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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
// FetchStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#fetchstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "FetchStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum FetchStepDef {
    #[serde(rename_all = "camelCase")]
    Url(#[serde_as(as = "FetchStepUrlDef")] FetchStepUrl),
    #[serde(rename_all = "camelCase")]
    FilesGlob(#[serde_as(as = "FetchStepFilesGlobDef")] FetchStepFilesGlob),
}

impl serde_with::SerializeAs<FetchStep> for FetchStepDef {
    fn serialize_as<S>(source: &FetchStep, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "FetchStepDef")] &'a FetchStep);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, FetchStep> for FetchStepDef {
    fn deserialize_as<D>(deserializer: D) -> Result<FetchStep, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "FetchStepDef")] FetchStep);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<FetchStepUrl> for FetchStepUrlDef {
    fn serialize_as<S>(source: &FetchStepUrl, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "FetchStepUrlDef")] &'a FetchStepUrl);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, FetchStepUrl> for FetchStepUrlDef {
    fn deserialize_as<D>(deserializer: D) -> Result<FetchStepUrl, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "FetchStepUrlDef")] FetchStepUrl);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<FetchStepFilesGlob> for FetchStepFilesGlobDef {
    fn serialize_as<S>(source: &FetchStepFilesGlob, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "FetchStepFilesGlobDef")] &'a FetchStepFilesGlob);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, FetchStepFilesGlob> for FetchStepFilesGlobDef {
    fn deserialize_as<D>(deserializer: D) -> Result<FetchStepFilesGlob, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "FetchStepFilesGlobDef")] FetchStepFilesGlob);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "SourceOrdering")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum SourceOrderingDef {
    ByEventTime,
    ByName,
}

impl serde_with::SerializeAs<SourceOrdering> for SourceOrderingDef {
    fn serialize_as<S>(source: &SourceOrdering, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "SourceOrderingDef")] &'a SourceOrdering);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, SourceOrdering> for SourceOrderingDef {
    fn deserialize_as<D>(deserializer: D) -> Result<SourceOrdering, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "SourceOrderingDef")] SourceOrdering);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

////////////////////////////////////////////////////////////////////////////////
// PrepStep
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#prepstep-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "PrepStep")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum PrepStepDef {
    #[serde(rename_all = "camelCase")]
    Decompress(#[serde_as(as = "PrepStepDecompressDef")] PrepStepDecompress),
    #[serde(rename_all = "camelCase")]
    Pipe(#[serde_as(as = "PrepStepPipeDef")] PrepStepPipe),
}

impl serde_with::SerializeAs<PrepStep> for PrepStepDef {
    fn serialize_as<S>(source: &PrepStep, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "PrepStepDef")] &'a PrepStep);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, PrepStep> for PrepStepDef {
    fn deserialize_as<D>(deserializer: D) -> Result<PrepStep, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "PrepStepDef")] PrepStep);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<PrepStepDecompress> for PrepStepDecompressDef {
    fn serialize_as<S>(source: &PrepStepDecompress, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "PrepStepDecompressDef")] &'a PrepStepDecompress);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, PrepStepDecompress> for PrepStepDecompressDef {
    fn deserialize_as<D>(deserializer: D) -> Result<PrepStepDecompress, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "PrepStepDecompressDef")] PrepStepDecompress);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<PrepStepPipe> for PrepStepPipeDef {
    fn serialize_as<S>(source: &PrepStepPipe, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "PrepStepPipeDef")] &'a PrepStepPipe);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, PrepStepPipe> for PrepStepPipeDef {
    fn deserialize_as<D>(deserializer: D) -> Result<PrepStepPipe, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "PrepStepPipeDef")] PrepStepPipe);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

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

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "CompressionFormat")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CompressionFormatDef {
    Gzip,
    Zip,
}

impl serde_with::SerializeAs<CompressionFormat> for CompressionFormatDef {
    fn serialize_as<S>(source: &CompressionFormat, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "CompressionFormatDef")] &'a CompressionFormat);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, CompressionFormat> for CompressionFormatDef {
    fn deserialize_as<D>(deserializer: D) -> Result<CompressionFormat, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "CompressionFormatDef")] CompressionFormat);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

////////////////////////////////////////////////////////////////////////////////
// EventTimeSource
// https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#eventtimesource-schema
////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSource")]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub enum EventTimeSourceDef {
    #[serde(rename_all = "camelCase")]
    FromMetadata,
    #[serde(rename_all = "camelCase")]
    FromPath(#[serde_as(as = "EventTimeSourceFromPathDef")] EventTimeSourceFromPath),
}

impl serde_with::SerializeAs<EventTimeSource> for EventTimeSourceDef {
    fn serialize_as<S>(source: &EventTimeSource, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "EventTimeSourceDef")] &'a EventTimeSource);
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, EventTimeSource> for EventTimeSourceDef {
    fn deserialize_as<D>(deserializer: D) -> Result<EventTimeSource, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "EventTimeSourceDef")] EventTimeSource);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

impl serde_with::SerializeAs<EventTimeSourceFromPath> for EventTimeSourceFromPathDef {
    fn serialize_as<S>(source: &EventTimeSourceFromPath, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(
            #[serde(with = "EventTimeSourceFromPathDef")] &'a EventTimeSourceFromPath,
        );
        Helper(source).serialize(serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, EventTimeSourceFromPath> for EventTimeSourceFromPathDef {
    fn deserialize_as<D>(deserializer: D) -> Result<EventTimeSourceFromPath, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "EventTimeSourceFromPathDef")] EventTimeSourceFromPath);
        let helper = Helper::deserialize(deserializer)?;
        let Helper(v) = helper;
        Ok(v)
    }
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(remote = "EventTimeSourceFromPath")]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventTimeSourceFromPathDef {
    pub pattern: String,
    pub timestamp_format: Option<String>,
}
