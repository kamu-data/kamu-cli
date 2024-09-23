// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use http_common::comma_separated::CommaSeparatedSet;
use http_common::{ApiError, IntoApiError};
use internal_error::*;
use kamu::domain;
use kamu_core::{DataFusionError, QueryError};
use opendatafabric::{self as odf, Multihash};

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
    pub private_key: odf::PrivateKey,
}

impl IdentityConfig {
    pub fn did(&self) -> odf::DidKey {
        odf::DidKey::new_ed25519(&self.private_key.verifying_key())
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

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
pub enum RequestBody {
    V1(RequestBodyV1),
    V2(RequestBodyV2),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestBodyV2 {
    /// Query string
    pub query: String,

    /// Dialect of the query
    #[serde(default = "RequestBodyV2::default_query_dialect")]
    pub query_dialect: domain::QueryDialect,

    /// How data should be layed out in the response
    #[serde(default)]
    pub data_format: DataFormat,

    /// What information to include
    #[serde(default = "RequestBodyV2::default_include")]
    pub include: BTreeSet<Include>,

    /// What representation to use for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_format: Option<SchemaFormat>,

    /// Optional information used to affix an alias to the specific
    /// [`odf::DatasetID`] and reproduce the query at a specific state in time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<DatasetState>>,

    /// Pagination: skips first N records
    #[serde(default)]
    pub skip: u64,

    /// Pagination: limits number of records in response to N
    #[serde(default = "RequestBodyV2::default_limit")]
    pub limit: u64,
}

impl RequestBodyV2 {
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
        let empty = Vec::new();
        let datasets = self.datasets.as_ref().unwrap_or(&empty);

        domain::QueryOptions {
            input_datasets: datasets
                .iter()
                .cloned()
                .map(|i| {
                    (
                        i.id,
                        domain::QueryOptionsDataset {
                            alias: i.alias,
                            block_hash: i.block_hash,
                            hints: None,
                        },
                    )
                })
                .collect(),
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

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestParams {
    pub query: String,

    #[serde(default = "RequestBodyV2::default_query_dialect")]
    pub query_dialect: domain::QueryDialect,

    #[serde(default)]
    pub skip: u64,

    #[serde(default = "RequestBodyV1::default_limit")]
    pub limit: u64,

    #[serde(alias = "format")]
    #[serde(default)]
    pub data_format: DataFormat,

    pub schema_format: Option<SchemaFormat>,

    // TODO: Remove after V2 transition
    #[serde(alias = "schema")]
    #[serde(default = "RequestBodyV1::default_include_schema")]
    pub include_schema: bool,

    // TODO: Remove after V2 transition
    #[serde(default = "RequestBodyV1::default_include_state")]
    pub include_state: bool,

    // TODO: Remove after V2 transition
    #[serde(default = "RequestBodyV1::default_include_data_hash")]
    pub include_data_hash: bool,

    /// What information to include in the response
    pub include: Option<CommaSeparatedSet<Include>>,
}

impl From<RequestParams> for RequestBody {
    fn from(v: RequestParams) -> Self {
        if let Some(include) = v.include {
            Self::V2(RequestBodyV2 {
                query: v.query,
                query_dialect: v.query_dialect,
                data_format: v.data_format,
                include: include.into(),
                schema_format: v.schema_format,
                datasets: None,
                skip: v.skip,
                limit: v.limit,
            })
        } else {
            Self::V1(RequestBodyV1 {
                query: v.query,
                data_format: v.data_format,
                schema_format: v.schema_format.unwrap_or_default(),
                include_schema: v.include_schema,
                include_state: v.include_state,
                skip: v.skip,
                limit: v.limit,
                include_data_hash: v.include_data_hash,
                aliases: None,
                as_of_state: None,
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Response
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum ResponseBody {
    V1(ResponseBodyV1),
    V2(ResponseBodyV2),
    V2Signed(ResponseBodyV2Signed),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBodyV2 {
    /// Inputs that can be used to fully reproduce the query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<RequestBodyV2>,

    /// Query results
    pub output: Outputs,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBodyV2Signed {
    /// Inputs that can be used to fully reproduce the query
    pub input: RequestBodyV2,

    /// Query results
    pub output: Outputs,

    /// Information about processing performed by other nodes as part of this
    /// operation
    pub sub_queries: Vec<SubQuery>,

    /// Succinct commitment
    pub commitment: Commitment,

    /// Signature block
    pub proof: Proof,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Fragments
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Mirrors the structure of [`ResponseBodyV2Signed`] without the `outputs`
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubQuery {
    /// Inputs that can be used to fully reproduce the query
    pub input: RequestBodyV2,

    /// Information about processing performed by other nodes as part of this
    /// operation
    pub sub_queries: Vec<SubQuery>,

    /// Succinct commitment
    pub commitment: Commitment,

    /// Signature block
    pub proof: Proof,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Outputs {
    /// Resulting data
    pub data: serde_json::Value,

    /// How data is layed out in the response
    pub data_format: DataFormat,

    /// Schema of the resulting data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,

    /// What representation is used for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_format: Option<SchemaFormat>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Commitment {
    /// Hash of the "input" object in the [multihash](https://multiformats.io/multihash/) format
    pub input_hash: Multihash,

    /// Hash of the "output" object in the [multihash](https://multiformats.io/multihash/) format
    pub output_hash: Multihash,

    /// Hash of the "subQueries" object in the [multihash](https://multiformats.io/multihash/) format
    pub sub_queries_hash: Multihash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Proof {
    /// Type of the proof provided
    pub r#type: ProofType,

    /// DID (public key) of the node performing the computation
    pub verification_method: odf::DidKey,

    /// Signature: `multibase(sign(canonicalize(commitment)))`
    pub proof_value: odf::Signature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ProofType {
    Ed25519Signature2020,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Reproducibility
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetState {
    /// Globally unique identity of the dataset
    pub id: odf::DatasetID,

    /// Alias to be used in the query
    pub alias: String,

    /// Last block hash of the input datasets that was or should be considered
    /// during the query planning
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    use kamu_data_utils::data::format::*;

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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone)]
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
        use kamu_data_utils::schema::{convert, format};

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Remove after V2 transition
pub(crate) fn serialize_schema(
    schema: &datafusion::arrow::datatypes::Schema,
    format: SchemaFormat,
) -> Result<String, InternalError> {
    use kamu_data_utils::schema::{convert, format};

    let mut buf = Vec::new();
    match format {
        SchemaFormat::ArrowJson => format::write_schema_arrow_json(&mut buf, schema),
        SchemaFormat::Parquet => format::write_schema_parquet(
            &mut buf,
            convert::arrow_schema_to_parquet_schema(schema).as_ref(),
        ),
        SchemaFormat::ParquetJson => format::write_schema_parquet_json(
            &mut buf,
            convert::arrow_schema_to_parquet_schema(schema).as_ref(),
        ),
    }
    .int_err()?;

    Ok(String::from_utf8(buf).unwrap())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_query_error(err: QueryError) -> ApiError {
    match err {
        QueryError::DatasetNotFound(_)
        | QueryError::DatasetBlockNotFound(_)
        | QueryError::DatasetSchemaNotAvailable(_) => ApiError::not_found(err),
        QueryError::DataFusionError(DataFusionError {
            source: datafusion::error::DataFusionError::SQL(err, _),
            ..
        }) => ApiError::bad_request(err),
        QueryError::DataFusionError(DataFusionError {
            source: err @ datafusion::error::DataFusionError::Plan(_),
            ..
        }) => ApiError::bad_request(err),
        QueryError::DataFusionError(_) => err.int_err().api_err(),
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
// Legacy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestBodyV1 {
    /// SQL query
    pub query: String,

    /// How data should be layed out in the response
    #[serde(default)]
    pub data_format: DataFormat,

    /// What representation to use for the schema
    #[serde(default)]
    pub schema_format: SchemaFormat,

    /// Mapping between dataset names used in the query and their stable IDs, to
    /// make query resistant to datasets being renamed
    pub aliases: Option<Vec<DatasetAlias>>,

    /// State information used to reproduce query at a specific point in time
    pub as_of_state: Option<QueryState>,

    /// Whether to include schema info about the response
    #[serde(default = "RequestBodyV1::default_include_schema")]
    pub include_schema: bool,

    /// Whether to include dataset state info for query reproducibility
    #[serde(default = "RequestBodyV1::default_include_state")]
    pub include_state: bool,

    /// Whether to include a logical hash of the resulting data batch.
    /// See: <https://docs.kamu.dev/odf/spec/#physical-and-logical-hashes>
    #[serde(default = "RequestBodyV1::default_include_data_hash")]
    pub include_data_hash: bool,

    /// Pagination: skips first N records
    #[serde(default)]
    pub skip: u64,

    /// Pagination: limits number of records in response to N
    #[serde(default = "RequestBodyV1::default_limit")]
    pub limit: u64,
}

impl RequestBodyV1 {
    fn default_limit() -> u64 {
        100
    }

    fn default_include_schema() -> bool {
        true
    }

    fn default_include_state() -> bool {
        true
    }

    fn default_include_data_hash() -> bool {
        true
    }

    pub fn to_options(&self) -> domain::QueryOptions {
        let empty_aliases = Vec::new();
        let aliases = self.aliases.as_ref().unwrap_or(&empty_aliases);

        let empty_hashes = Vec::new();
        let block_hashes = self
            .as_of_state
            .as_ref()
            .map_or(&empty_hashes, |s| &s.inputs);

        let block_hashes: BTreeMap<_, _> = block_hashes
            .iter()
            .map(|s| (s.id.clone(), s.block_hash.clone()))
            .collect();

        domain::QueryOptions {
            input_datasets: aliases
                .iter()
                .map(|a| {
                    (
                        a.id.clone(),
                        domain::QueryOptionsDataset {
                            alias: a.alias.clone(),
                            block_hash: block_hashes.get(&a.id).cloned(),
                            hints: None,
                        },
                    )
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBodyV1 {
    pub data: Box<serde_json::value::RawValue>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<QueryState>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_hash: Option<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatasetAlias {
    pub id: odf::DatasetID,
    pub alias: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryState {
    pub inputs: Vec<QueryDatasetState>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryDatasetState {
    pub id: odf::DatasetID,
    pub block_hash: odf::Multihash,
}

impl From<kamu_core::QueryState> for QueryState {
    fn from(value: kamu_core::QueryState) -> Self {
        Self {
            inputs: value
                .input_datasets
                .into_iter()
                .map(|(id, s)| QueryDatasetState {
                    id,
                    block_hash: s.block_hash,
                })
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
