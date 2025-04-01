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
// Config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Move into a more appropriate layer
#[derive(Debug, Clone)]
pub struct IdentityConfig {
    /// Root private key that corresponds to the `authority` and is used to sign
    /// responses
    ///
    /// To generate use:
    ///
    /// ```sh
    /// ssh-keygen -t ed25519 -C "coo@abc.com"
    /// ```
    pub private_key: odf::metadata::PrivateKey,
}

impl IdentityConfig {
    pub fn did(&self) -> odf::metadata::DidKey {
        odf::metadata::DidKey::new_ed25519(&self.private_key.verifying_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_config_doesnt_leak_key() {
        let cfg = IdentityConfig {
            private_key: ed25519_dalek::SigningKey::from_bytes(
                &[123; ed25519_dalek::SECRET_KEY_LENGTH],
            )
            .into(),
        };

        assert_eq!(
            format!("{cfg:?}"),
            "IdentityConfig { private_key: PrivateKey(***) }"
        );
    }
}

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
    pub r#type: ProofType,

    /// DID (public key) of the node performing the computation
    pub verification_method: odf::metadata::DidKey,

    /// Signature: `multibase(sign(canonicalize(commitment)))`
    pub proof_value: odf::metadata::Signature,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
pub enum ProofType {
    Ed25519Signature2020,
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

#[allow(clippy::enum_variant_names)]
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

#[allow(clippy::enum_variant_names)]
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
    #[serde(alias = "parquet")]
    Parquet,
    #[serde(alias = "parquetjson")]
    #[serde(alias = "parquet-json")]
    ParquetJson,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Schema {
    schema: datafusion::arrow::datatypes::SchemaRef,
    format: SchemaFormat,
}

impl Schema {
    pub fn new(schema: datafusion::arrow::datatypes::SchemaRef, format: SchemaFormat) -> Self {
        Self { schema, format }
    }
}

impl serde::Serialize for Schema {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use odf::utils::schema::{convert, format};

        match self.format {
            SchemaFormat::ArrowJson => self.schema.serialize(serializer),
            SchemaFormat::ParquetJson => {
                let mut buf = Vec::new();

                format::write_schema_parquet_json(
                    &mut buf,
                    convert::arrow_schema_to_parquet_schema(&self.schema).as_ref(),
                )
                .unwrap();

                // TODO: PERF: Avoid re-serialization
                let json: serde_json::Value = serde_json::from_slice(&buf).unwrap();
                json.serialize(serializer)
            }
            SchemaFormat::Parquet => {
                let mut buf = Vec::new();

                format::write_schema_parquet(
                    &mut buf,
                    convert::arrow_schema_to_parquet_schema(&self.schema).as_ref(),
                )
                .unwrap();

                serializer.collect_str(&std::str::from_utf8(&buf).unwrap())
            }
        }
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
        QueryError::DatasetNotFound(_)
        | QueryError::DatasetBlockNotFound(_)
        | QueryError::DatasetSchemaNotAvailable(_) => ApiError::not_found(err),
        QueryError::BadQuery(_) => ApiError::bad_request(err),
        QueryError::Access(err) => err.api_err(),
        QueryError::Internal(err) => err.api_err(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn to_canonical_json<T: serde::Serialize>(val: &T) -> Vec<u8> {
    // TODO: PERF: Avoid double-serialization
    // Because `canonical_json` needs `serde_json::Value` to be able to sort keys
    // before serializing into a buffer we end up paying double for allocations.
    // A better approach would be to use a macro to enforce alphabetic order of
    // struct fields to avoid the need of sorting and then use a canonical formatter
    // to write directly to a buffer.
    let json = serde_json::to_value(val).unwrap();
    let str = canonical_json::to_string(&json).unwrap();
    str.into_bytes()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
