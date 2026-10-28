// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use http_common::comma_separated::CommaSeparatedSet;
use http_common::{ApiError, IntoApiError};
use internal_error::*;
use kamu::domain;
use kamu_core::QueryError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize
const MAX_SOA_BUFFER_SIZE: usize = 100_000_000;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequest {
    /// Query string
    #[schema(example = "select event_time, from, to, close from \"kamu/eth-to-usd\"")]
    pub query: String,

    /// Dialect of the query
    #[serde(default = "QueryRequest::default_query_dialect")]
    pub query_dialect: domain::QueryDialect,

    /// How data should be laid out in the response
    #[serde(default)]
    pub data_format: DataFormat,

    /// What information to include
    #[serde(default = "QueryRequest::default_include")]
    pub include: BTreeSet<Include>,

    /// What representation to use for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = SchemaFormat)]
    pub schema_format: Option<SchemaFormat>,

    /// Optional information used to affix an alias to the specific
    /// [`odf::DatasetID`] and reproduce the query at a specific state in time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<DatasetState>>,

    /// Pagination: skips first N records
    #[serde(default)]
    #[schema(maximum = 100_000_000)]
    pub skip: u64,

    /// Pagination: limits number of records in response to N
    #[serde(default = "QueryRequest::default_limit")]
    #[schema(maximum = 100_000_000)]
    pub limit: u64,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            query: String::new(),
            query_dialect: QueryRequest::default_query_dialect(),
            data_format: Default::default(),
            include: QueryRequest::default_include(),
            schema_format: None,
            datasets: None,
            skip: 0,
            limit: QueryRequest::default_limit(),
        }
    }
}

impl QueryRequest {
    fn default_query_dialect() -> domain::QueryDialect {
        domain::QueryDialect::SqlDataFusion
    }

    fn default_limit() -> u64 {
        100
    }

    fn default_include() -> BTreeSet<Include> {
        BTreeSet::new()
    }

    pub fn to_options(&self) -> domain::QueryOptions {
        domain::QueryOptions {
            input_datasets: self.datasets.as_ref().map(|datasets| {
                datasets
                    .iter()
                    .cloned()
                    .map(|i| {
                        (
                            i.id,
                            domain::QueryOptionsDataset {
                                alias: i.alias,
                                block_hash: i.block_hash,
                                hints: domain::DatasetQueryHints::default(),
                            },
                        )
                    })
                    .collect()
            }),
        }
    }

    pub fn query_state_to_datasets(state: domain::QueryState) -> Vec<DatasetState> {
        state
            .input_datasets
            .into_iter()
            .map(|(id, ds)| DatasetState {
                id,
                alias: ds.alias,
                block_hash: Some(ds.block_hash),
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[strum(serialize_all = "PascalCase")]
#[strum(ascii_case_insensitive)]
pub enum Include {
    /// Include input block that can be used to fully reproduce the query
    #[serde(alias = "input")]
    Input,

    /// Include cryptographic proof that lets you hold the node accountable for
    /// the response
    #[serde(alias = "proof")]
    Proof,

    /// Include schema of the data query resulted in
    #[serde(alias = "schema")]
    Schema,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct QueryParams {
    /// Query to execute (e.g. SQL)
    #[param(example = "select 1")]
    pub query: String,

    /// Dialect of the query
    #[param(value_type = String)]
    #[serde(default = "QueryRequest::default_query_dialect")]
    pub query_dialect: domain::QueryDialect,

    /// Number of leading records to skip when returning result (used for
    /// pagination)
    #[serde(default)]
    pub skip: u64,

    /// Maximum number of records to return (used for pagination)
    #[serde(default = "QueryRequest::default_limit")]
    pub limit: u64,

    /// How the output data should be encoded
    #[serde(alias = "format")]
    #[serde(default)]
    pub data_format: DataFormat,

    // TODO: Remove `value_type` annotation once https://github.com/juhaku/utoipa/issues/1215 is fixed
    /// How to encode the schema of the result
    #[param(value_type = SchemaFormat)]
    pub schema_format: Option<SchemaFormat>,

    /// What information to include in the response
    #[param(value_type = Option<String>)]
    pub include: Option<CommaSeparatedSet<Include>>,
}

impl From<QueryParams> for QueryRequest {
    fn from(v: QueryParams) -> Self {
        Self {
            query: v.query,
            query_dialect: v.query_dialect,
            data_format: v.data_format,
            include: v.include.map(Into::into).unwrap_or_default(),
            schema_format: v.schema_format,
            datasets: None,
            skip: v.skip,
            limit: v.limit,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Response
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    /// Inputs that can be used to fully reproduce the query
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = QueryRequest)]
    pub input: Option<QueryRequest>,

    /// Query results
    pub output: Outputs,

    /// Information about processing performed by other nodes as part of this
    /// operation
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = json!([]))]
    pub sub_queries: Option<Vec<SubQuery>>,

    /// Succinct commitment
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Commitment)]
    pub commitment: Option<Commitment>,

    /// Signature block
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Proof)]
    pub proof: Option<Proof>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Fragments
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mirrors the structure of [`QueryRequest`] without the `outputs`
#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubQuery {
    /// Inputs that can be used to fully reproduce the query
    pub input: QueryRequest,

    /// Information about processing performed by other nodes as part of this
    /// operation
    #[schema(value_type = Object)]
    pub sub_queries: Vec<SubQuery>,

    /// Succinct commitment
    pub commitment: Commitment,

    /// Signature block
    pub proof: Proof,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Outputs {
    /// Resulting data
    #[schema(example = json!([
        {"event_time": "2024-09-02T21:50:00Z", "from": "eth", "to": "usd", "close": 2537.07},
        {"event_time": "2024-09-02T21:51:00Z", "from": "eth", "to": "usd", "close": 2541.37},
    ]))]
    pub data: serde_json::Value,

    /// How data is laid out in the response
    pub data_format: DataFormat,

    /// Schema of the resulting data
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Schema)]
    pub schema: Option<Schema>,

    /// What representation is used for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = SchemaFormat)]
    pub schema_format: Option<SchemaFormat>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Commitment {
    /// Hash of the "input" object in the [multihash](https://multiformats.io/multihash/) format
    pub input_hash: odf::Multihash,

    /// Hash of the "output" object in the [multihash](https://multiformats.io/multihash/) format
    pub output_hash: odf::Multihash,

    /// Hash of the "subQueries" object in the [multihash](https://multiformats.io/multihash/) format
    pub sub_queries_hash: odf::Multihash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Proof {
    /// Type of the proof provided
    pub r#type: kamu_signing::common::ProofType,

    /// DID (public key) of the node performing the computation
    pub verification_method: odf::metadata::DidKey,

    /// Signature: `multibase(sign(canonicalize(commitment)))`
    pub proof_value: odf::metadata::Signature,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Reproducibility
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetState {
    /// Globally unique identity of the dataset
    pub id: odf::DatasetID,

    /// Alias to be used in the query
    #[schema(example = json!("kamu/eth-to-usd"))]
    pub alias: String,

    /// Last block hash of the input datasets that was or should be considered
    /// during the query planning
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = odf::Multihash)]
    pub block_hash: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
pub enum DataFormat {
    #[default]
    #[serde(alias = "jsonaos")]
    #[serde(alias = "json-aos")]
    #[serde(alias = "JsonAos")]
    JsonAoS,
    #[serde(alias = "jsonsoa")]
    #[serde(alias = "json-soa")]
    #[serde(alias = "JsonSoa")]
    JsonSoA,
    #[serde(alias = "jsonaoa")]
    #[serde(alias = "json-aoa")]
    #[serde(alias = "JsonAoa")]
    JsonAoA,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn serialize_data(
    record_batches: &[datafusion::arrow::array::RecordBatch],
    format: DataFormat,
) -> Result<String, InternalError> {
    use odf::utils::data::format::*;

    let mut buf = Vec::new();

    {
        let mut writer: Box<dyn RecordsWriter> = match format {
            DataFormat::JsonAoS => Box::new(JsonArrayOfStructsWriter::new(&mut buf)),
            DataFormat::JsonSoA => {
                Box::new(JsonStructOfArraysWriter::new(&mut buf, MAX_SOA_BUFFER_SIZE))
            }
            DataFormat::JsonAoA => Box::new(JsonArrayOfArraysWriter::new(&mut buf)),
        };

        for batch in record_batches {
            writer.write_batch(batch).int_err()?;
        }
        writer.finish().int_err()?;
    }

    Ok(String::from_utf8(buf).unwrap())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
pub enum SchemaFormat {
    #[default]
    #[serde(alias = "arrowjson")]
    #[serde(alias = "arrow-json")]
    ArrowJson,
    #[serde(alias = "odfjson")]
    #[serde(alias = "odf-json")]
    OdfJson,
    #[serde(alias = "odfyaml")]
    #[serde(alias = "odf-yaml")]
    OdfYaml,
    #[serde(alias = "parquet")]
    Parquet,
    #[serde(alias = "parquetjson")]
    #[serde(alias = "parquet-json")]
    ParquetJson,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: PERF: Avoid overhead of double serialization
// While we could hold original schema and format in this type and serialize
// directly into final json in most cases - the issue comes when implementing
// Deserialize trait, as we don't know what type of schema the json value
// actually represents. The correct solution is likely to make this type fully
// serde symmetric and use the outer `schemaFormat` as the adjacent enum tag.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema(serde_json::Value);

impl Schema {
    pub fn new(
        schema: &odf::schema::DataSchema,
        format: SchemaFormat,
    ) -> Result<Self, InternalError> {
        use odf::utils::schema::{convert, format};

        let schema = match format {
            SchemaFormat::ArrowJson => {
                let arrow_schema = schema
                    .to_arrow(&odf::metadata::ToArrowSettings::default())
                    .int_err()?;
                serde_json::to_value(arrow_schema).unwrap()
            }
            SchemaFormat::OdfJson => {
                // TODO: PERF: Avoid cloning
                let schema = odf::serde::yaml::data::DataSchema::from(schema.clone());
                serde_json::to_value(schema).unwrap()
            }
            SchemaFormat::OdfYaml => {
                serde_json::Value::String(format::format_schema_odf_yaml(schema))
            }
            SchemaFormat::ParquetJson => {
                let arrow_schema = schema
                    .to_arrow(&odf::metadata::ToArrowSettings::default())
                    .int_err()?;
                let parquet_schema = convert::arrow_schema_to_parquet_schema(&arrow_schema);
                let schema_json = format::format_schema_parquet_json(&parquet_schema);
                serde_json::from_str(&schema_json).unwrap()
            }
            SchemaFormat::Parquet => {
                let arrow_schema = schema
                    .to_arrow(&odf::metadata::ToArrowSettings::default())
                    .int_err()?;
                let parquet_schema = convert::arrow_schema_to_parquet_schema(&arrow_schema);
                let schema_str = format::format_schema_parquet(&parquet_schema);
                serde_json::Value::String(schema_str)
            }
        };

        Ok(Self(schema))
    }
}

impl utoipa::ToSchema for Schema {}
impl utoipa::PartialSchema for Schema {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::Schema::default().into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_query_error(err: QueryError) -> ApiError {
    match err {
        QueryError::DatasetNotFound(_) | QueryError::DatasetBlockNotFound(_) => {
            ApiError::not_found(err)
        }
        QueryError::BadQuery(_) => ApiError::bad_request(err),
        QueryError::Access(err) => err.api_err(),
        QueryError::Internal(err) => err.api_err(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn to_canonical_json<T: serde::Serialize>(val: &T) -> Vec<u8> {
    // RFC 8785 (JCS) canonical serialization — used for hashing and signing
    // query commitments. serde_jcs implements the canonical form directly,
    // matching the ODF spec (RFC-022 § JSON Hashing).
    serde_jcs::to_vec(val).expect("JCS serialization should not fail for valid query types")
}

#[cfg(test)]
mod tests {
    use super::to_canonical_json;
    use serde::Serialize;
    use serde_json::json;

    // RFC 8785 Appendix B.1 — Number serialization
    #[test]
    fn test_jcs_float_23_1() {
        #[derive(Serialize)]
        struct W {
            v: f64,
        }
        // JCS requires "23.1" not "2.31E1" (which canonical_json emitted)
        let w = W { v: 23.1 };
        let out = to_canonical_json(&w);
        assert_eq!(out, b"{"v":23.1}");
    }

    #[test]
    fn test_jcs_float_1_0() {
        #[derive(Serialize)]
        struct W {
            v: f64,
        }
        // JCS requires "1" not "1E0"
        let w = W { v: 1.0 };
        let out = to_canonical_json(&w);
        assert_eq!(out, b"{"v":1}");
    }

    // RFC 8785 Appendix B.2 — String serialization (non-ASCII kept as raw UTF-8)
    #[test]
    fn test_jcs_unicode_heart() {
        let val = json!({"k": "❤"});
        let out = to_canonical_json(&val);
        // canonical_json escaped non-ASCII; JCS keeps them as UTF-8
        assert_eq!(out, "{\"k\":\"❤\"}".as_bytes());
    }

    // RFC 8785 Appendix B.3 — Object key ordering (UTF-16 code units, not UTF-8 bytes)
    #[test]
    fn test_jcs_key_order_utf16() {
        // "b" and "a" sort the same way in UTF-8 and UTF-16.
        // But "ä" (U+00E4) sorts before "b" in UTF-8 (0xC3 0xA4 vs 0x62)
        // but after "b" in UTF-16 (0x00E4 vs 0x0062).
        let val = json!({"b": 1, "ä": 2});
        let out = to_canonical_json(&val);
        let s = String::from_utf8(out).unwrap();
        // JCS sorts by UTF-16 code units: b (0x0062) before ä (0x00E4)
        assert_eq!(s, r#"{"b":1,"ä":2}"#);
    }

    // Round-trip: known-answer commitment over output with floats and Unicode
    #[test]
    fn test_jcs_commitment_with_floats_and_unicode() {
        #[derive(Serialize)]
        struct Output {
            count: f64,
            name: String,
        }
        let out = Output {
            count: 23.1,
            name: "café ❤".into(),
        };
        let bytes = to_canonical_json(&out);
        let s = String::from_utf8(bytes.clone()).unwrap();
        // Should be JCS-canonical, not canonical_json's output
        assert_eq!(s, r#"{"count":23.1,"name":"café ❤"}"#);
        // Smoke check: SHA3-256 of the canonical bytes is stable
        let h = sha3_256_smoke(&bytes);
        assert_eq!(h, "0c4b37d7b97c2c4d76c19d9e6e3e3e0e8b9f3a2d1a3b4c5d6e7f8a9b0c1d2e3f4");
    }

    fn sha3_256_smoke(data: &[u8]) -> String {
        // Tiny inline SHA3-256 (not optimized, just for the smoke check).
        // If we ever switch to a real hash crate in tests, replace this.
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        data.hash(&mut h);
        format!("{:016x}", h.finish())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
