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
pub trait KamuOdataApi: Send + Sync {
    /// GET /odata/{account_name}/{dataset_name}
    ///
    async fn odata_collection_handler_mt(
        &self,
        params: OdataCollectionHandlerMtParams,
    ) -> Result<String, Error<OdataCollectionHandlerMtError>>;

    /// GET /odata/{account_name}/$metadata
    ///
    async fn odata_metadata_handler_mt(
        &self,
        params: OdataMetadataHandlerMtParams,
    ) -> Result<String, Error<OdataMetadataHandlerMtError>>;

    /// GET /odata/{account_name}
    ///
    async fn odata_service_handler_mt(
        &self,
        params: OdataServiceHandlerMtParams,
    ) -> Result<String, Error<OdataServiceHandlerMtError>>;
}

pub struct KamuOdataApiClient {
    configuration: Arc<configuration::Configuration>,
}

impl KamuOdataApiClient {
    pub fn new(configuration: Arc<configuration::Configuration>) -> Self {
        Self { configuration }
    }
}

/// struct for passing parameters to the method [`odata_collection_handler_mt`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct OdataCollectionHandlerMtParams {
    /// Account name
    pub account_name: String,
    /// Dataset name
    pub dataset_name: String,
}

/// struct for passing parameters to the method [`odata_metadata_handler_mt`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct OdataMetadataHandlerMtParams {
    /// Account name
    pub account_name: String,
}

/// struct for passing parameters to the method [`odata_service_handler_mt`]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "bon", derive(::bon::Builder))]
pub struct OdataServiceHandlerMtParams {
    /// Account name
    pub account_name: String,
}

#[async_trait]
impl KamuOdataApi for KamuOdataApiClient {
    async fn odata_collection_handler_mt(
        &self,
        params: OdataCollectionHandlerMtParams,
    ) -> Result<String, Error<OdataCollectionHandlerMtError>> {
        let OdataCollectionHandlerMtParams {
            account_name,
            dataset_name,
        } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!(
            "{}/odata/{account_name}/{dataset_name}",
            local_var_configuration.base_path,
            account_name = crate::apis::urlencode(account_name),
            dataset_name = crate::apis::urlencode(dataset_name)
        );
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

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
                ContentType::Text => return Ok(local_var_content),
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `String`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<OdataCollectionHandlerMtError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    async fn odata_metadata_handler_mt(
        &self,
        params: OdataMetadataHandlerMtParams,
    ) -> Result<String, Error<OdataMetadataHandlerMtError>> {
        let OdataMetadataHandlerMtParams { account_name } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!(
            "{}/odata/{account_name}/$metadata",
            local_var_configuration.base_path,
            account_name = crate::apis::urlencode(account_name)
        );
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

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
                ContentType::Text => return Ok(local_var_content),
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `String`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<OdataMetadataHandlerMtError> =
                serde_json::from_str(&local_var_content).ok();
            let local_var_error = ResponseContent {
                status: local_var_status,
                content: local_var_content,
                entity: local_var_entity,
            };
            Err(Error::ResponseError(local_var_error))
        }
    }

    async fn odata_service_handler_mt(
        &self,
        params: OdataServiceHandlerMtParams,
    ) -> Result<String, Error<OdataServiceHandlerMtError>> {
        let OdataServiceHandlerMtParams { account_name } = params;

        let local_var_configuration = &self.configuration;

        let local_var_client = &local_var_configuration.client;

        let local_var_uri_str = format!(
            "{}/odata/{account_name}",
            local_var_configuration.base_path,
            account_name = crate::apis::urlencode(account_name)
        );
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

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
                ContentType::Text => return Ok(local_var_content),
                ContentType::Unsupported(local_var_unknown_type) => {
                    return Err(Error::from(serde_json::Error::custom(format!(
                        "Received `{local_var_unknown_type}` content type response that cannot be \
                         converted to `String`"
                    ))))
                }
            }
        } else {
            let local_var_entity: Option<OdataServiceHandlerMtError> =
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

/// struct for typed errors of method [`odata_collection_handler_mt`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OdataCollectionHandlerMtError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`odata_metadata_handler_mt`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OdataMetadataHandlerMtError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`odata_service_handler_mt`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OdataServiceHandlerMtError {
    UnknownValue(serde_json::Value),
}
