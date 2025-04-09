// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/*
 * Kamu REST API
 *
 * You are currently running Kamu CLI in the API server mode. For a fully-featured server consider using [Kamu Node](https://docs.kamu.dev/node/).  ## Auth Some operation require an **API token**. Pass `--get-token` command line argument for CLI to generate a token for you.  ## Resources - [Documentation](https://docs.kamu.dev) - [Discord](https://discord.gg/nU6TXRQNXC) - [Other protocols](https://docs.kamu.dev/node/protocols/) - [Open Data Fabric specification](https://docs.kamu.dev/odf/)
 *
 * The version of the OpenAPI document: 0.233.0
 *
 * Generated by: https://openapi-generator.tech
 */

use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "mockall")]
use mockall::automock;
use reqwest;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use super::{configuration, Error};
use crate::apis::{ContentType, ResponseContent};
use crate::models;

#[cfg_attr(feature = "mockall", automock)]
#[async_trait]
pub trait OdfQueryApi: Send + Sync {
    /// GET /{account_name}/{dataset_name}/metadata
    ///
    async fn dataset_metadata_handler(
        &self,
        params: DatasetMetadataHandlerParams,
    ) -> Result<models::DatasetMetadataResponse, Error<DatasetMetadataHandlerError>>;

    /// GET /{account_name}/{dataset_name}/tail
    ///
    async fn dataset_tail_handler(
        &self,
        params: DatasetTailHandlerParams,
    ) -> Result<models::DatasetTailResponse, Error<DatasetTailHandlerError>>;

    /// GET /query
    ///
    /// Functions exactly like the [POST version](#tag/odf-query/POST/query) of
    /// the endpoint with all parameters passed in the query string instead of
    /// the body.
    async fn query_handler(
        &self,
        params: QueryHandlerParams,
    ) -> Result<models::QueryResponse, Error<QueryHandlerError>>;

    /// POST /query
    ///
    /// ### Regular Queries This endpoint lets you execute arbitrary SQL that can access multiple datasets at once.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\" } ```  Example response: ```json {     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     } } ```  ### Verifiable Queries [Cryptographic proofs](https://docs.kamu.dev/node/commitments) can be also requested to hold the node **forever accountable** for the provided result.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\",     \"include\": [\"proof\"] } ```  Currently, we support verifiability by ensuring that queries are deterministic and fully reproducible and signing the original response with Node's private key. In future more types of proofs will be supported.  Example response: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f1620708557a44c88d23c83f2b915abc10a41cc38d2a278e851e5dc6bb02b7e1f9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f1620e23f7d8cdde7504eadb86f3cdf34b3b1a7d71f10fe5b54b528dd803387422efc\",         \"outputHash\": \"f1620e91f4d3fa26bc4ca0c49d681c8b630550239b64d3cbcfd7c6c2d6ff45998b088\",         \"subQueriesHash\": \"f1620ca4510738395af1429224dd785675309c344b2b549632e20275c69b15ed1d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6MkkhJQPHpA41mTPLFgBeygnjeeADUSwuGDoF9pbGQsfwZp\",         \"proofValue\": \"uJfY3_g03WbmqlQG8TL-WUxKYU8ZoJaP14MzOzbnJedNiu7jpoKnCTNnDI3TYuaXv89vKlirlGs-5AN06mBseCg\"     } } ```  A client that gets a proof in response should perform [a few basic steps](https://docs.kamu.dev/node/commitments#response-validation) to validate the proof integrity. For example making sure that the DID in `proof.verificationMethod` actually corresponds to the node you're querying data from and that the signature in `proof.proofValue` is actually valid. Only after this you can use this proof to hold the node accountable for the result.  A proof can be stored long-term and then disputed at a later point using your own node or a 3rd party node you can trust via the [`/verify`](#tag/odf-query/POST/verify) endpoint.  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
    async fn query_handler_post(
        &self,
        params: QueryHandlerPostParams,
    ) -> Result<models::QueryResponse, Error<QueryHandlerPostError>>;

    /// POST /verify
    ///
    /// A query proof can be stored long-term and then disputed at a later point using this endpoint.  Example request: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0..26c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f162..9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f162..2efc\",         \"outputHash\": \"f162..b088\",         \"subQueriesHash\": \"f162..d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6Mk..fwZp\",         \"proofValue\": \"uJfY..seCg\"     } } ```  Example response: ```json {     \"ok\": false,     \"error\": {         \"kind\": \"VerificationFailed::OutputMismatch\",         \"actual_hash\": \"f162..c12a\",         \"expected_hash\": \"f162..2a2d\",         \"message\": \"Query was reproduced but resulted in output hash different from expected.                     This means that the output was either falsified, or the query                     reproducibility was not guaranteed by the system.\",     } } ```  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
    async fn verify_handler(
        &self,
        params: VerifyHandlerParams,
    ) -> Result<models::VerifyResponse, Error<VerifyHandlerError>>;
}

pub struct OdfQueryApiClient {
    configuration: Arc<configuration::Configuration>,
}

impl OdfQueryApiClient {
    pub fn new(configuration: Arc<configuration::Configuration>) -> Self {
        Self { configuration }
    }
}

/// struct for passing parameters to the method [`dataset_metadata_handler`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct DatasetMetadataHandlerParams {
    /// Name of the account
    pub account_name: String,
    /// Name of the dataset
    pub dataset_name: String,
    /// What information to include in response
    pub include: Option<String>,
    /// Format to return the schema in
    pub schema_format: Option<models::SchemaFormat>,
}

/// struct for passing parameters to the method [`dataset_tail_handler`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct DatasetTailHandlerParams {
    /// How to encode the schema of the result
    pub schema_format: models::SchemaFormat,
    /// Name of the account
    pub account_name: String,
    /// Name of the dataset
    pub dataset_name: String,
    /// Number of leading records to skip when returning result (used for
    /// pagination)
    pub skip: Option<i64>,
    /// Maximum number of records to return (used for pagination)
    pub limit: Option<i64>,
    /// How the output data should be encoded
    pub data_format: Option<models::DataFormat>,
}

/// struct for passing parameters to the method [`query_handler`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct QueryHandlerParams {
    /// Query to execute (e.g. SQL)
    pub query: String,
    /// How to encode the schema of the result
    pub schema_format: models::SchemaFormat,
    /// Dialect of the query
    pub query_dialect: Option<String>,
    /// Number of leading records to skip when returning result (used for
    /// pagination)
    pub skip: Option<i64>,
    /// Maximum number of records to return (used for pagination)
    pub limit: Option<i64>,
    /// How the output data should be encoded
    pub data_format: Option<models::DataFormat>,
    /// What information to include in the response
    pub include: Option<String>,
}

/// struct for passing parameters to the method [`query_handler_post`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct QueryHandlerPostParams {
    pub query_request: models::QueryRequest,
}

/// struct for passing parameters to the method [`verify_handler`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct VerifyHandlerParams {
    pub verify_request: models::VerifyRequest,
}

#[async_trait]
impl OdfQueryApi for OdfQueryApiClient {
    async fn dataset_metadata_handler(
        &self,
        params: DatasetMetadataHandlerParams,
    ) -> Result<models::DatasetMetadataResponse, Error<DatasetMetadataHandlerError>> {
        let DatasetMetadataHandlerParams {
            account_name,
            dataset_name,
            include,
            schema_format,
        } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!(
            "{}/{account_name}/{dataset_name}/metadata",
            local_var_configuration.base_path,
            account_name = crate::apis::urlencode(account_name),
            dataset_name = crate::apis::urlencode(dataset_name)
        );
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

        if let Some(ref local_var_str) = include {
            local_var_req_builder =
                local_var_req_builder.query(&[("include", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = schema_format {
            local_var_req_builder =
                local_var_req_builder.query(&[("schemaFormat", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }
        if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content_type = local_var_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let local_var_content_type = super::ContentType::from(local_var_content_type);
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            match local_var_content_type {
                ContentType::Json => serde_json::from_str(&local_var_content).map_err(Error::from),
                ContentType::Text => {
                    return Err(Error::from(serde_json::Error::custom(
                        "Received `text/plain` content type response that cannot be converted to \
                         `models::DatasetMetadataResponse`",
                    )))
                }
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `models::DatasetMetadataResponse`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<DatasetMetadataHandlerError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    async fn dataset_tail_handler(
        &self,
        params: DatasetTailHandlerParams,
    ) -> Result<models::DatasetTailResponse, Error<DatasetTailHandlerError>> {
        let DatasetTailHandlerParams {
            schema_format,
            account_name,
            dataset_name,
            skip,
            limit,
            data_format,
        } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!(
            "{}/{account_name}/{dataset_name}/tail",
            local_var_configuration.base_path,
            account_name = crate::apis::urlencode(account_name),
            dataset_name = crate::apis::urlencode(dataset_name)
        );
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

        if let Some(ref local_var_str) = skip {
            local_var_req_builder =
                local_var_req_builder.query(&[("skip", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = limit {
            local_var_req_builder =
                local_var_req_builder.query(&[("limit", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = data_format {
            local_var_req_builder =
                local_var_req_builder.query(&[("dataFormat", &local_var_str.to_string())]);
        }
        local_var_req_builder =
            local_var_req_builder.query(&[("schemaFormat", &schema_format.to_string())]);
        if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }
        if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content_type = local_var_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let local_var_content_type = super::ContentType::from(local_var_content_type);
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            match local_var_content_type {
                ContentType::Json => serde_json::from_str(&local_var_content).map_err(Error::from),
                ContentType::Text => {
                    return Err(Error::from(serde_json::Error::custom(
                        "Received `text/plain` content type response that cannot be converted to \
                         `models::DatasetTailResponse`",
                    )))
                }
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `models::DatasetTailResponse`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<DatasetTailHandlerError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    /// Functions exactly like the [POST version](#tag/odf-query/POST/query) of
    /// the endpoint with all parameters passed in the query string instead of
    /// the body.
    async fn query_handler(
        &self,
        params: QueryHandlerParams,
    ) -> Result<models::QueryResponse, Error<QueryHandlerError>> {
        let QueryHandlerParams {
            query,
            schema_format,
            query_dialect,
            skip,
            limit,
            data_format,
            include,
        } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!("{}/query", local_var_configuration.base_path);
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

        local_var_req_builder = local_var_req_builder.query(&[("query", &query.to_string())]);
        if let Some(ref local_var_str) = query_dialect {
            local_var_req_builder =
                local_var_req_builder.query(&[("queryDialect", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = skip {
            local_var_req_builder =
                local_var_req_builder.query(&[("skip", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = limit {
            local_var_req_builder =
                local_var_req_builder.query(&[("limit", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_str) = data_format {
            local_var_req_builder =
                local_var_req_builder.query(&[("dataFormat", &local_var_str.to_string())]);
        }
        local_var_req_builder =
            local_var_req_builder.query(&[("schemaFormat", &schema_format.to_string())]);
        if let Some(ref local_var_str) = include {
            local_var_req_builder =
                local_var_req_builder.query(&[("include", &local_var_str.to_string())]);
        }
        if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }
        if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content_type = local_var_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let local_var_content_type = super::ContentType::from(local_var_content_type);
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            match local_var_content_type {
                ContentType::Json => serde_json::from_str(&local_var_content).map_err(Error::from),
                ContentType::Text => {
                    return Err(Error::from(serde_json::Error::custom(
                        "Received `text/plain` content type response that cannot be converted to \
                         `models::QueryResponse`",
                    )))
                }
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `models::QueryResponse`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<QueryHandlerError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    /// ### Regular Queries This endpoint lets you execute arbitrary SQL that can access multiple datasets at once.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\" } ```  Example response: ```json {     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     } } ```  ### Verifiable Queries [Cryptographic proofs](https://docs.kamu.dev/node/commitments) can be also requested to hold the node **forever accountable** for the provided result.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\",     \"include\": [\"proof\"] } ```  Currently, we support verifiability by ensuring that queries are deterministic and fully reproducible and signing the original response with Node's private key. In future more types of proofs will be supported.  Example response: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f1620708557a44c88d23c83f2b915abc10a41cc38d2a278e851e5dc6bb02b7e1f9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f1620e23f7d8cdde7504eadb86f3cdf34b3b1a7d71f10fe5b54b528dd803387422efc\",         \"outputHash\": \"f1620e91f4d3fa26bc4ca0c49d681c8b630550239b64d3cbcfd7c6c2d6ff45998b088\",         \"subQueriesHash\": \"f1620ca4510738395af1429224dd785675309c344b2b549632e20275c69b15ed1d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6MkkhJQPHpA41mTPLFgBeygnjeeADUSwuGDoF9pbGQsfwZp\",         \"proofValue\": \"uJfY3_g03WbmqlQG8TL-WUxKYU8ZoJaP14MzOzbnJedNiu7jpoKnCTNnDI3TYuaXv89vKlirlGs-5AN06mBseCg\"     } } ```  A client that gets a proof in response should perform [a few basic steps](https://docs.kamu.dev/node/commitments#response-validation) to validate the proof integrity. For example making sure that the DID in `proof.verificationMethod` actually corresponds to the node you're querying data from and that the signature in `proof.proofValue` is actually valid. Only after this you can use this proof to hold the node accountable for the result.  A proof can be stored long-term and then disputed at a later point using your own node or a 3rd party node you can trust via the [`/verify`](#tag/odf-query/POST/verify) endpoint.  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
    async fn query_handler_post(
        &self,
        params: QueryHandlerPostParams,
    ) -> Result<models::QueryResponse, Error<QueryHandlerPostError>> {
        let QueryHandlerPostParams { query_request } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!("{}/query", local_var_configuration.base_path);
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::POST, local_var_uri_str.as_str());

        if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }
        if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        local_var_req_builder = local_var_req_builder.json(&query_request);

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content_type = local_var_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let local_var_content_type = super::ContentType::from(local_var_content_type);
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            match local_var_content_type {
                ContentType::Json => serde_json::from_str(&local_var_content).map_err(Error::from),
                ContentType::Text => {
                    return Err(Error::from(serde_json::Error::custom(
                        "Received `text/plain` content type response that cannot be converted to \
                         `models::QueryResponse`",
                    )))
                }
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `models::QueryResponse`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<QueryHandlerPostError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    /// A query proof can be stored long-term and then disputed at a later point using this endpoint.  Example request: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0..26c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f162..9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f162..2efc\",         \"outputHash\": \"f162..b088\",         \"subQueriesHash\": \"f162..d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6Mk..fwZp\",         \"proofValue\": \"uJfY..seCg\"     } } ```  Example response: ```json {     \"ok\": false,     \"error\": {         \"kind\": \"VerificationFailed::OutputMismatch\",         \"actual_hash\": \"f162..c12a\",         \"expected_hash\": \"f162..2a2d\",         \"message\": \"Query was reproduced but resulted in output hash different from expected.                     This means that the output was either falsified, or the query                     reproducibility was not guaranteed by the system.\",     } } ```  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
    async fn verify_handler(
        &self,
        params: VerifyHandlerParams,
    ) -> Result<models::VerifyResponse, Error<VerifyHandlerError>> {
        let VerifyHandlerParams { verify_request } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!("{}/verify", local_var_configuration.base_path);
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::POST, local_var_uri_str.as_str());

        if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }
        if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        local_var_req_builder = local_var_req_builder.json(&verify_request);

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content_type = local_var_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        let local_var_content_type = super::ContentType::from(local_var_content_type);
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            match local_var_content_type {
                ContentType::Json => serde_json::from_str(&local_var_content).map_err(Error::from),
                ContentType::Text => {
                    return Err(Error::from(serde_json::Error::custom(
                        "Received `text/plain` content type response that cannot be converted to \
                         `models::VerifyResponse`",
                    )))
                }
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `models::VerifyResponse`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<VerifyHandlerError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }
}

/// struct for typed errors of method [`dataset_metadata_handler`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetMetadataHandlerError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`dataset_tail_handler`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetTailHandlerError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`query_handler`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryHandlerError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`query_handler_post`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryHandlerPostError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`verify_handler`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VerifyHandlerError {
    Status400(models::ApiErrorResponse),
    UnknownValue(serde_json::Value),
}
