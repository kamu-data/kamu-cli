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

use std::fmt::Debug;

use axum::extract::{self, Extension, Query};
use axum::response::Json;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use kamu::domain::*;
use kamu_data_utils::data::format::*;
use opendatafabric::{DatasetID, Multihash};

use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize
const MAX_SOA_BUFFER_SIZE: usize = 100_000_000;

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_query_handler_post(
    Extension(catalog): Extension<dill::Catalog>,
    extract::Json(body): extract::Json<QueryRequestBody>,
) -> Result<Json<QueryResponseBody>, ApiError> {
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let res = match query_svc
        .sql_statement(&body.query, body.to_options())
        .await
    {
        Ok(res) => res,
        Err(QueryError::DatasetNotFound(err)) => Err(ApiError::not_found(err))?,
        Err(QueryError::DataFusionError(err @ DataFusionError::Plan(_))) => {
            Err(ApiError::bad_request(err))?
        }
        Err(e) => Err(e.int_err().api_err())?,
    };

    // Apply pagination limits
    let df = res
        .df
        .limit(
            usize::try_from(body.skip).unwrap(),
            Some(usize::try_from(body.limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    let arrow_schema = df.schema().inner().clone();

    let schema = if body.include_schema {
        Some(serialize_schema(df.schema(), body.schema_format).api_err()?)
    } else {
        None
    };

    let state = if body.include_state {
        Some(res.state.into())
    } else {
        None
    };

    let record_batches = df.collect().await.int_err().api_err()?;
    let json = serialize_data(&record_batches, body.data_format).api_err()?;
    let data = serde_json::value::RawValue::from_string(json).unwrap();

    let data_hash = if body.include_data_hash {
        Some(kamu_data_utils::data::hash::get_batches_logical_hash(
            &arrow_schema,
            &record_batches,
        ))
    } else {
        None
    };

    Ok(Json(QueryResponseBody {
        data,
        schema,
        state,
        data_hash,
    }))
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_query_handler(
    catalog: Extension<dill::Catalog>,
    Query(params): Query<QueryRequestParams>,
) -> Result<Json<QueryResponseBody>, ApiError> {
    dataset_query_handler_post(catalog, extract::Json(params.into())).await
}

/////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequestBody {
    /// SQL query
    query: String,

    /// How data should be layed out in the response
    #[serde(default)]
    data_format: DataFormat,

    /// What representation to use for the schema
    #[serde(default)]
    schema_format: SchemaFormat,

    /// Mapping between dataset names used in the query and their stable IDs, to
    /// make query resistant to datasets being renamed
    aliases: Option<Vec<QueryDatasetAlias>>,

    /// State information used to reproduce query at a specific point in time
    as_of_state: Option<QueryState>,

    /// Whether to include schema info about the response
    #[serde(default = "QueryRequestBody::default_include_schema")]
    include_schema: bool,

    /// Whether to include dataset state info for query reproducibility
    #[serde(default = "QueryRequestBody::default_include_state")]
    include_state: bool,

    /// Whether to include a logical hash of the resulting data batch.
    /// See: https://docs.kamu.dev/odf/spec/#physical-and-logical-hashes
    #[serde(default = "QueryRequestBody::default_include_data_hash")]
    include_data_hash: bool,

    /// Pagination: skips first N records
    #[serde(default)]
    skip: u64,
    /// Pagination: limits number of records in response to N
    #[serde(default = "QueryRequestBody::default_limit")]
    limit: u64,
}

impl QueryRequestBody {
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

    fn to_options(&self) -> QueryOptions {
        QueryOptions {
            aliases: self
                .aliases
                .as_ref()
                .map(|v| v.iter().map(|i| (i.alias.clone(), i.id.clone())).collect()),
            as_of_state: self.as_of_state.as_ref().map(QueryState::to_state),
            hints: None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryRequestParams {
    query: String,
    #[serde(default)]
    skip: u64,
    #[serde(default = "QueryRequestBody::default_limit")]
    limit: u64,
    #[serde(alias = "format")]
    #[serde(default)]
    data_format: DataFormat,
    #[serde(default)]
    schema_format: SchemaFormat,
    #[serde(alias = "schema")]
    #[serde(default = "QueryRequestBody::default_include_schema")]
    include_schema: bool,
    #[serde(default = "QueryRequestBody::default_include_state")]
    include_state: bool,
    #[serde(default = "QueryRequestBody::default_include_data_hash")]
    include_data_hash: bool,
}

impl From<QueryRequestParams> for QueryRequestBody {
    fn from(v: QueryRequestParams) -> Self {
        Self {
            query: v.query,
            data_format: v.data_format,
            schema_format: v.schema_format,
            aliases: None,
            as_of_state: None,
            include_schema: v.include_schema,
            include_state: v.include_state,
            include_data_hash: v.include_data_hash,
            skip: v.skip,
            limit: v.limit,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponseBody {
    data: Box<serde_json::value::RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<QueryState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_hash: Option<Multihash>,
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryDatasetAlias {
    pub alias: String,
    pub id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryState {
    pub inputs: Vec<QueryDatasetState>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryDatasetState {
    pub id: DatasetID,
    pub block_hash: Multihash,
}

impl QueryState {
    fn to_state(&self) -> kamu::domain::QueryState {
        kamu::domain::QueryState {
            inputs: self
                .inputs
                .iter()
                .map(|i| (i.id.clone(), i.block_hash.clone()))
                .collect(),
        }
    }
}

impl From<kamu::domain::QueryState> for QueryState {
    fn from(value: kamu::domain::QueryState) -> Self {
        Self {
            inputs: value
                .inputs
                .into_iter()
                .map(|(id, block_hash)| QueryDatasetState { id, block_hash })
                .collect(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub enum DataFormat {
    #[default]
    #[serde(alias = "jsonaos")]
    #[serde(alias = "json-aos")]
    JsonAos,
    #[serde(alias = "jsonsoa")]
    #[serde(alias = "json-soa")]
    JsonSoa,
    #[serde(alias = "jsonaoa")]
    #[serde(alias = "json-aoa")]
    JsonAoa,
}

/////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
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

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn serialize_schema(
    schema: &DFSchema,
    format: SchemaFormat,
) -> Result<String, InternalError> {
    use kamu_data_utils::schema::{convert, format};

    let mut buf = Vec::new();
    match format {
        SchemaFormat::ArrowJson => {
            let arrow_schema = datafusion::arrow::datatypes::Schema::from(schema);
            format::write_schema_arrow_json(&mut buf, &arrow_schema)
        }
        SchemaFormat::Parquet => format::write_schema_parquet(
            &mut buf,
            convert::dataframe_schema_to_parquet_schema(schema).as_ref(),
        ),
        SchemaFormat::ParquetJson => format::write_schema_parquet_json(
            &mut buf,
            convert::dataframe_schema_to_parquet_schema(schema).as_ref(),
        ),
    }
    .int_err()?;

    Ok(String::from_utf8(buf).unwrap())
}

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn serialize_data(
    record_batches: &[RecordBatch],
    format: DataFormat,
) -> Result<String, InternalError> {
    let mut buf = Vec::new();

    {
        let mut writer: Box<dyn RecordsWriter> = match format {
            DataFormat::JsonAos => Box::new(JsonArrayOfStructsWriter::new(&mut buf)),
            DataFormat::JsonSoa => {
                Box::new(JsonStructOfArraysWriter::new(&mut buf, MAX_SOA_BUFFER_SIZE))
            }
            DataFormat::JsonAoa => Box::new(JsonArrayOfArraysWriter::new(&mut buf)),
        };

        for batch in record_batches {
            writer.write_batch(batch).int_err()?;
        }
        writer.finish().int_err()?;
    }

    Ok(String::from_utf8(buf).unwrap())
}

/////////////////////////////////////////////////////////////////////////////////
