// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;
use kamu_search::SearchIndexUpdateOperation;
use reqwest::Method;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};

use super::*;
use crate::ElasticsearchClientConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MAX_LOG_SIZE_CHARS: usize = 1000;

const DEFAULT_ELASTICSEARCH_USER: &str = "elastic";

const ES_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const ES_PROPAGATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const ES_SHARD_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchClient {
    client: reqwest::Client,
    base_url: String,
    password: Option<String>,
    enable_compression: bool,
}

#[common_macros::method_names_consts]
impl ElasticsearchClient {
    #[tracing::instrument(level = "info", name = ElasticsearchClient_init, skip_all)]
    pub fn init(config: &ElasticsearchClientConfig) -> Result<Self, ElasticsearchClientBuildError> {
        tracing::info!(config = ?config, "Initializing Elasticsearch client");

        let mut builder = reqwest::ClientBuilder::new()
            .no_proxy() // often desirable in k8s
            .timeout(std::time::Duration::from_secs(config.timeout_secs));

        // Configure TLS certificate if provided
        if let Some(ca_cert_path) = &config.ca_cert_pem_path {
            tracing::info!(ca_cert_path = ?ca_cert_path, "Loading CA certificate for Elasticsearch client");
            let cert_pem = std::fs::read(ca_cert_path).map_err(|e| {
                ElasticsearchClientBuildError::CertificateRead {
                    path: ca_cert_path.clone(),
                    source: e,
                }
            })?;

            let cert = reqwest::Certificate::from_pem(&cert_pem)
                .map_err(ElasticsearchClientBuildError::InvalidCertificate)?;
            builder = builder.add_root_certificate(cert);
        }

        let client = builder
            .build()
            .map_err(ElasticsearchClientBuildError::Build)?;

        Ok(Self {
            client,
            base_url: config.url.as_str().trim_end_matches('/').to_owned(),
            password: config.password.clone(),
            enable_compression: config.enable_compression,
        })
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_cluster_health, skip_all)]
    pub async fn cluster_health(&self) -> Result<serde_json::Value, ElasticsearchClientError> {
        let response = self
            .send_request(Method::GET, "/_cluster/health", RequestPayload::None)
            .await?;

        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;
        Ok(body)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_total_documents, skip_all)]
    pub async fn total_documents(&self) -> Result<u64, ElasticsearchClientError> {
        let response = self
            .send_request(Method::GET, "/_count", RequestPayload::None)
            .await?;

        let response = ensure_client_response(response).await?;
        let body: CountResponse = response.json().await?;
        Ok(body.count)
    }

    #[tracing::instrument(
        level = "debug",
        name = ElasticsearchClient_documents_in_index,
        skip_all,
        fields(index_name)
    )]
    pub async fn documents_in_index(
        &self,
        index_name: &str,
    ) -> Result<u64, ElasticsearchClientError> {
        let response = self
            .send_request(
                Method::GET,
                &format!("/{index_name}/_count"),
                RequestPayload::None,
            )
            .await?;

        let response = ensure_client_response(response).await?;
        let body: CountResponse = response.json().await?;
        Ok(body.count)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_search, skip_all)]
    pub async fn search(
        &self,
        req_body: serde_json::Value,
        index_names: &[&str],
    ) -> Result<SearchResponse, ElasticsearchClientError> {
        tracing::debug!(
            index_names = ?index_names,
            req_body = Self::sanitize_json_for_logging(&req_body, MAX_LOG_SIZE_CHARS),
            "Executing Elasticsearch request"
        );
        /*println!(
            "\nES Search request: {}, indexes: {index_names:?}\n",
            serde_json::to_string_pretty(&req_body).unwrap()
        );*/

        // Send request to Elasticsearch
        let response = self
            .send_request(
                Method::POST,
                &format!("/{}/_search", index_names.join(",")),
                RequestPayload::Json(&req_body),
            )
            .await?;

        // Obtain and log a raw response
        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;
        tracing::debug!(
            body = Self::sanitize_json_for_logging(&body, MAX_LOG_SIZE_CHARS),
            "Elasticsearch response"
        );
        /*println!(
            "\nRaw ES Search response: {}\n",
            serde_json::to_string_pretty(&body).unwrap()
        );*/

        // Try interpreting the response
        let search_response: SearchResponse = serde_json::from_value(body)?;
        Ok(search_response)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_find_document_by_id, skip_all, fields(index_name, id))]
    pub async fn find_document_by_id(
        &self,
        index_name: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>, ElasticsearchClientError> {
        let safe_id = urlencoding::encode(id);

        let response = self
            .send_request(
                Method::GET,
                &format!("/{index_name}/_doc/{safe_id}"),
                RequestPayload::None,
            )
            .await?;

        // 404 means document does not exist
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = ensure_client_response(response).await?;
        let body: GetDocumentByIdResponse = response.json().await?;
        if body.found {
            Ok(body.source)
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_list_indices_by_prefix, skip_all, fields(prefix))]
    pub async fn list_indices_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, ElasticsearchClientError> {
        let response = self
            .send_request(
                Method::GET,
                &format!("/{prefix}*/_mapping"),
                RequestPayload::None,
            )
            .await?;

        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;

        let mut indices = Vec::new();
        if let Some(obj) = body.as_object() {
            for (idx_name, _) in obj {
                indices.push(idx_name.clone());
            }
        }

        Ok(indices)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_refresh_indices, skip_all)]
    pub async fn refresh_indices(
        &self,
        index_names: &[&str],
    ) -> Result<(), ElasticsearchClientError> {
        if index_names.is_empty() {
            return Ok(());
        }

        tracing::debug!(index_names = ?index_names, "Refreshing Elasticsearch indices");

        let response = self
            .send_request(
                Method::POST,
                &format!("/{}/_refresh", index_names.join(",")),
                RequestPayload::None,
            )
            .await?;

        let _ = ensure_client_response(response).await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", name = ElasticsearchClient_create_index, skip_all, fields(index_name))]
    pub async fn create_index(
        &self,
        index_name: &str,
        body: serde_json::Value,
    ) -> Result<(), ElasticsearchClientError> {
        tracing::info!(index_name, body = ?body, "Creating Elasticsearch index");

        let response = self
            .send_request(
                Method::PUT,
                &format!("/{index_name}?wait_for_active_shards=1"),
                RequestPayload::Json(&body),
            )
            .await?;

        let _ = ensure_client_response(response).await?;

        // After create returns successfully,
        // wait until the index is actually queryable.
        self.wait_for_index_queryable(index_name).await?;

        Ok(())
    }

    /// Wait until the primary shard for a brand new index is started (or at
    /// least queryable).
    ///
    /// This is intentionally pragmatic: we probe with a cheap operation and
    /// retry on 503/no-shard.
    async fn wait_for_index_queryable(
        &self,
        index_name: &str,
    ) -> Result<(), ElasticsearchClientError> {
        // A cheap probe that exercises shard readiness
        self.retry_transient(
            "wait_for_index_queryable",
            ES_SHARD_READY_TIMEOUT,
            || async {
                // COUNT is cheap and sufficient to validate queryability.
                let _n = self.documents_in_index(index_name).await?;
                Ok(())
            },
        )
        .await
    }

    #[tracing::instrument(level = "info", name = ElasticsearchClient_delete_indices_bulk, skip_all)]
    pub async fn delete_indices_bulk(
        &self,
        index_names: &[&str],
    ) -> Result<(), ElasticsearchClientError> {
        if index_names.is_empty() {
            return Ok(());
        }

        tracing::info!(index_names = ?index_names, "Deleting Elasticsearch indices");

        let response = self
            .send_request(
                Method::DELETE,
                &format!("/{}", index_names.join(",")),
                RequestPayload::None,
            )
            .await?;

        let _ = ensure_client_response(response).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_read_index_meta, skip_all, fields(index_name))]
    pub async fn read_index_meta(
        &self,
        index_name: &str,
    ) -> Result<serde_json::Value, ElasticsearchClientError> {
        let response = self
            .send_request(
                Method::GET,
                &format!("/{index_name}/_mapping"),
                RequestPayload::None,
            )
            .await?;
        let response = ensure_client_response(response).await?;

        // Body structure: {
        //    "<index_name>": {
        //        "mappings": {
        //            "_meta": {
        //                ... interested metadata ...
        //            },
        //            "properties": {
        //                ... field mappings ...
        //            }
        //        }
        //    }
        // }
        let body: serde_json::Value = response.json().await?;
        let index_settings = &body[index_name]["mappings"]["_meta"];
        Ok(index_settings.clone())
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_assign_alias, skip_all, fields(alias_name, to_index))]
    pub async fn assign_alias(
        &self,
        alias_name: &str,
        to_index: &str,
        maybe_filter_json: Option<serde_json::Value>,
    ) -> Result<(), ElasticsearchClientError> {
        // Check if alias exists.
        // Removing non-existing alias may cause an error.
        let alias_exists = {
            let resp = self
                .send_request(
                    Method::HEAD,
                    &format!("/_alias/{alias_name}"),
                    RequestPayload::None,
                )
                .await?;
            resp.status() == reqwest::StatusCode::OK
        };

        // Build add alias action
        let mut add_command_json = serde_json::json!({
            "index": to_index,
            "alias": alias_name,
            "is_write_index": true,
        });
        if let Some(filter_json) = maybe_filter_json {
            add_command_json["filter"] = filter_json;
        }

        // Existing alias? Remove + add new.
        let actions = if alias_exists {
            serde_json::json!([
                { "remove": { "index": "*", "alias": alias_name } },
                { "add": add_command_json }
            ])
        } else {
            // New alias? Just add.
            serde_json::json!([
                { "add": add_command_json }
            ])
        };

        // Final alias assignment command body
        let body = serde_json::json!({ "actions": actions });

        tracing::debug!(
            alias_name,
            to_index,
            body = ?body,
            "Moving Elasticsearch alias to new index"
        );

        let response = self
            .send_request(Method::POST, "/_aliases", RequestPayload::Json(&body))
            .await?;

        let _ = ensure_client_response(response).await?;

        // Ensure cluster-state propagation:
        // callers can safely use alias immediately after.
        self.wait_for_alias(alias_name).await?;

        Ok(())
    }

    async fn wait_for_alias(&self, alias_name: &str) -> Result<(), ElasticsearchClientError> {
        // Wait until alias is visible in cluster state.
        self.retry_transient("wait_for_alias", ES_PROPAGATION_TIMEOUT, || async {
            let resp = self
                .send_request(
                    Method::HEAD,
                    &format!("/_alias/{alias_name}"),
                    RequestPayload::None,
                )
                .await?;

            match resp.status() {
                reqwest::StatusCode::OK => Ok(()),
                reqwest::StatusCode::NOT_FOUND => Err(ElasticsearchClientError::IndexNotFound {
                    index: alias_name.to_owned(),
                }),
                _ => {
                    let _ = ensure_client_response(resp).await?;
                    Ok(())
                }
            }
        })
        .await
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_resolve_alias_indices, skip_all, fields(alias_name))]
    pub async fn resolve_alias_indices(
        &self,
        alias_name: &str,
    ) -> Result<Vec<String>, ElasticsearchClientError> {
        let response = self
            .send_request(
                Method::GET,
                &format!("/_alias/{alias_name}"),
                RequestPayload::None,
            )
            .await?;

        // 404 means alias does not exist
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(vec![]);
        }

        // shape:
        // {
        //   "index1": { "aliases": { "<alias>": { ... } } },
        //   "index2": {...},  ...
        // }
        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;

        let mut indices = Vec::new();
        if let Some(obj) = body.as_object() {
            for (idx_name, v) in obj {
                if v["aliases"].get(alias_name).is_some() {
                    indices.push(idx_name.clone());
                }
            }
        }

        Ok(indices)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_reindex_data, skip_all, fields(from_index, to_index))]
    pub async fn reindex_data(
        &self,
        from_index: &str,
        to_index: &str,
    ) -> Result<(), ElasticsearchReindexError> {
        // Copy everything from source to dest
        let body = serde_json::json!({
            "source": {
                "index": from_index,
            },
            "dest": {
                "index": to_index,
            }
        });

        tracing::debug!(
            from_index,
            to_index,
            body = ?body,
            "Reindexing data from one Elasticsearch index to another"
        );

        let response = self
            .send_request(
                Method::POST,
                "/_reindex?wait_for_completion=true&refresh=true",
                RequestPayload::Json(&body),
            )
            .await
            .map_err(ElasticsearchReindexError::Client)?;

        let response = ensure_client_response(response)
            .await
            .map_err(ElasticsearchReindexError::Client)?;
        let value: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ElasticsearchReindexError::Client(e.into()))?;

        tracing::info!(
            "Reindex from '{}' to '{}' completed with response: {}",
            from_index,
            to_index,
            value
        );

        #[derive(serde::Deserialize)]
        struct ReindexSummary {
            updated: u64,
            deleted: u64,
            failures: Vec<serde_json::Value>,
        }

        let summary = serde_json::from_value::<ReindexSummary>(value)
            .map_err(ElasticsearchClientError::from)
            .map_err(ElasticsearchReindexError::Client)?;

        // Check for failures
        if !summary.failures.is_empty() {
            return Err(ElasticsearchReindexError::ReindexFailures(summary.failures));
        }

        // We only expect new documents to be created, no updates or deletions
        if summary.deleted > 0 || summary.updated > 0 {
            return Err(ElasticsearchReindexError::UnexpectedDocumentChanges {
                updated: summary.updated,
                deleted: summary.deleted,
            });
        }

        // Success
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        name = ElasticsearchClient_bulk_update,
        skip_all,
        fields(index_name, num_operations = operations.len())
    )]
    pub async fn bulk_update(
        &self,
        index_name: &str,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), ElasticsearchClientError> {
        if operations.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            index_name,
            num_operations = operations.len(),
            "Executing bulk update operation"
        );

        let bulk_body = build_bulk_request_body(operations)?;
        let response = self
            .send_request(
                Method::POST,
                &format!("/{index_name}/_bulk"),
                RequestPayload::Ndjson(bulk_body),
            )
            .await?;

        handle_bulk_response(response, index_name, "update").await
    }

    // Retry helper for short, bounded eventual-consistency windows in ES.
    async fn retry_transient<T, Fut, F>(
        &self,
        op_name: &'static str,
        timeout: std::time::Duration,
        mut f: F,
    ) -> Result<T, ElasticsearchClientError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ElasticsearchClientError>>,
    {
        let deadline = std::time::Instant::now() + timeout;
        let mut attempt_index: u32 = 0;
        let mut last_err: Option<ElasticsearchClientError> = None;

        loop {
            tracing::debug!(operation = op_name, attempt_index, "Attempting operation");
            match f().await {
                Ok(v) => {
                    tracing::debug!(operation = op_name, "Operation succeeded");
                    return Ok(v);
                }
                Err(e)
                    if Self::is_transient_es_error(&e) && std::time::Instant::now() < deadline =>
                {
                    tracing::warn!(
                        operation = op_name,
                        error = ?e,
                        deadline = ?deadline,
                        attempt_index,
                        "Transient Elasticsearch error detected, retrying..."
                    );

                    attempt_index += 1;
                    last_err = Some(e);

                    tokio::time::sleep(ES_POLL_INTERVAL).await;
                }
                Err(e) => {
                    // Keep the last transient error if we have one and we timed out.
                    tracing::error!(
                        operation = op_name,
                        error = ?e,
                        "Elasticsearch operation failed"
                    );
                    return Err(last_err.unwrap_or(e));
                }
            }
        }
    }

    fn is_transient_es_error(e: &ElasticsearchClientError) -> bool {
        match e {
            // Classic alias/index propagation window.
            ElasticsearchClientError::IndexNotFound { .. } => true,

            // Shard not started yet / initial allocation window.
            ElasticsearchClientError::BadResponse {
                status,
                error_type,
                body,
                ..
            } => {
                // 503 + typical error types for shard unavailability
                if *status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    if error_type == "search_phase_execution_exception"
                        || error_type == "no_shard_available_action_exception"
                    {
                        return true;
                    }

                    // Defensive: sometimes the root cause is only in the JSON body.
                    // Keep this lightweight (no deep parsing assumptions).
                    let s = body.to_string();
                    if s.contains("no_shard_available_action_exception")
                        || s.contains("all shards failed")
                        || s.contains("primary shard is not active")
                    {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// Sanitize JSON for logging by truncating large arrays (embeddings) and
    /// limiting total size
    fn sanitize_json_for_logging(value: &serde_json::Value, max_chars: usize) -> String {
        fn truncate_arrays(value: &serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::Array(arr) => {
                    // If array has more than 3 elements and looks like a vector (all numbers)
                    if arr.len() > 3
                        && arr
                            .iter()
                            .all(|v| matches!(v, serde_json::Value::Number(_)))
                    {
                        // This is likely an embedding vector - show only first 3 elements
                        let truncated: Vec<_> = arr.iter().take(3).cloned().collect();
                        serde_json::json!({
                            "truncated_vector": truncated,
                            "total_dims": arr.len()
                        })
                    } else {
                        // Recursively process normal arrays
                        serde_json::Value::Array(arr.iter().map(truncate_arrays).collect())
                    }
                }
                serde_json::Value::Object(obj) => serde_json::Value::Object(
                    obj.iter()
                        .map(|(k, v)| (k.clone(), truncate_arrays(v)))
                        .collect(),
                ),
                other => other.clone(),
            }
        }

        let sanitized = truncate_arrays(value);
        let json_str = serde_json::to_string(&sanitized).unwrap_or_else(|_| "{}".to_string());

        if json_str.len() > max_chars {
            format!(
                "{}... (truncated, total {} chars)",
                &json_str[..max_chars],
                json_str.len()
            )
        } else {
            json_str
        }
    }

    async fn send_request(
        &self,
        method: Method,
        path: &str,
        payload: RequestPayload<'_>,
    ) -> Result<reqwest::Response, ElasticsearchClientError> {
        let mut builder = self.client.request(method, self.request_url(path));

        if let Some(password) = &self.password {
            builder = builder.basic_auth(DEFAULT_ELASTICSEARCH_USER, Some(password));
        }

        builder = match payload {
            RequestPayload::None => builder,
            RequestPayload::Json(value) => {
                let body = serde_json::to_vec(value)?;
                self.attach_body(builder, body, "application/json")?
            }
            RequestPayload::Ndjson(body) => {
                self.attach_body(builder, body, "application/x-ndjson")?
            }
        };

        builder
            .send()
            .await
            .map_err(ElasticsearchClientError::Transport)
    }

    fn attach_body(
        &self,
        builder: reqwest::RequestBuilder,
        body: Vec<u8>,
        content_type: &'static str,
    ) -> Result<reqwest::RequestBuilder, ElasticsearchClientError> {
        let body = if self.enable_compression {
            compress_body(&body)?
        } else {
            body
        };

        let builder = builder.header(CONTENT_TYPE, content_type);

        if self.enable_compression {
            Ok(builder.header(CONTENT_ENCODING, "gzip").body(body))
        } else {
            Ok(builder.body(body))
        }
    }

    fn request_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum RequestPayload<'a> {
    None,
    Json(&'a serde_json::Value),
    Ndjson(Vec<u8>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticsearchReindexError {
    #[error("Elasticsearch client error: {0}")]
    Client(#[from] ElasticsearchClientError),

    #[error("Detected unexpected document updates or deletions during reindexing")]
    UnexpectedDocumentChanges { updated: u64, deleted: u64 },

    #[error("Reindexing failed with failures: {0:?}")]
    ReindexFailures(Vec<serde_json::Value>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticsearchClientBuildError {
    #[error("transport error building Elasticsearch client: {0}")]
    Build(reqwest::Error),

    #[error("failed to read CA certificate from {path}: {source}")]
    CertificateRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("invalid certificate format: {0}")]
    InvalidCertificate(reqwest::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticsearchClientError {
    #[error("transport error communicating to Elasticsearch: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("failed to serialize Elasticsearch request/response payload: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("failed to compress Elasticsearch request body: {0}")]
    Compression(#[from] std::io::Error),

    #[error("Elasticsearch error {status} ({error_type}): {reason}")]
    BadResponse {
        status: reqwest::StatusCode,
        error_type: String,
        reason: String,
        body: serde_json::Value,
    },

    #[error("Elasticsearch index not found: {index}")]
    IndexNotFound { index: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
struct BulkResponse {
    errors: bool,
    items: Vec<serde_json::Value>,
}

async fn handle_bulk_response(
    response: reqwest::Response,
    index_name: &str,
    operation_name: &str,
) -> Result<(), ElasticsearchClientError> {
    let response = ensure_client_response(response).await?;
    let bulk_result: BulkResponse = response.json().await?;

    if bulk_result.errors {
        tracing::error!(
            index_name,
            items = ?bulk_result.items,
            "Bulk {operation_name} completed with errors"
        );
        return Err(ElasticsearchClientError::BadResponse {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            error_type: format!("bulk_{operation_name}_errors"),
            reason: format!("Some documents failed to {operation_name}"),
            body: serde_json::json!({ "items": bulk_result.items }),
        });
    }

    tracing::debug!(
        index_name,
        "Bulk {operation_name} operation completed successfully"
    );
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn ensure_client_response(
    resp: reqwest::Response,
) -> Result<reqwest::Response, ElasticsearchClientError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }

    let body_text = resp.text().await.unwrap_or_default();

    Err(parse_error_response(status, &body_text))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_error_response(status: reqwest::StatusCode, body_text: &str) -> ElasticsearchClientError {
    #[derive(serde::Deserialize)]
    struct EsErrorRoot {
        error: EsErrorDetail,
    }

    #[derive(serde::Deserialize)]
    struct EsErrorDetail {
        #[serde(rename = "type")]
        type_: String,
        reason: String,
    }

    // Try JSON first
    if let Ok(parsed) = serde_json::from_str::<EsErrorRoot>(body_text) {
        let error_type = parsed.error.type_;
        let reason = parsed.error.reason;
        let body_json =
            serde_json::from_str::<serde_json::Value>(body_text).unwrap_or(serde_json::Value::Null);

        // Special case: index_not_found
        const INDEX_NOT_FOUND_ERROR_TYPE: &str = "index_not_found_exception";
        if error_type == INDEX_NOT_FOUND_ERROR_TYPE {
            let index = body_json["error"]["index"]
                .as_str()
                .unwrap_or_default()
                .to_string();

            return ElasticsearchClientError::IndexNotFound { index };
        }

        return ElasticsearchClientError::BadResponse {
            status,
            error_type,
            reason,
            body: body_json,
        };
    }

    // If it wasn't JSON, still return a structured error
    ElasticsearchClientError::BadResponse {
        status,
        error_type: "non_json_error".to_string(),
        reason: body_text.to_string(),
        body: serde_json::Value::String(body_text.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn build_bulk_request_body(
    operations: Vec<SearchIndexUpdateOperation>,
) -> Result<Vec<u8>, ElasticsearchClientError> {
    let mut body = Vec::new();

    for op in operations {
        match op {
            SearchIndexUpdateOperation::Index { id, doc } => {
                body.extend(serde_json::to_vec(&serde_json::json!({
                    "index": { "_id": id }
                }))?);
                body.push(b'\n');
                body.extend(serde_json::to_vec(&doc)?);
                body.push(b'\n');
            }
            SearchIndexUpdateOperation::Update { id, doc } => {
                body.extend(serde_json::to_vec(&serde_json::json!({
                    "update": { "_id": id }
                }))?);
                body.push(b'\n');
                body.extend(serde_json::to_vec(&serde_json::json!({
                    "doc": doc
                }))?);
                body.push(b'\n');
            }
            SearchIndexUpdateOperation::Delete { id } => {
                body.extend(serde_json::to_vec(&serde_json::json!({
                    "delete": { "_id": id }
                }))?);
                body.push(b'\n');
            }
        }
    }

    Ok(body)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn compress_body(body: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body)?;
    encoder.finish()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_bulk_request_body_uses_ndjson_and_trailing_newline() {
        let body = build_bulk_request_body(vec![
            SearchIndexUpdateOperation::Index {
                id: "doc-1".to_string(),
                doc: serde_json::json!({"foo":"bar"}),
            },
            SearchIndexUpdateOperation::Update {
                id: "doc-2".to_string(),
                doc: serde_json::json!({"baz":"qux"}),
            },
            SearchIndexUpdateOperation::Delete {
                id: "doc-3".to_string(),
            },
        ])
        .unwrap();

        let body = String::from_utf8(body).unwrap();
        assert!(body.ends_with('\n'));

        let lines: Vec<_> = body.lines().collect();
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[0], r#"{"index":{"_id":"doc-1"}}"#);
        assert_eq!(lines[1], r#"{"foo":"bar"}"#);
        assert_eq!(lines[2], r#"{"update":{"_id":"doc-2"}}"#);
        assert_eq!(lines[3], r#"{"doc":{"baz":"qux"}}"#);
        assert_eq!(lines[4], r#"{"delete":{"_id":"doc-3"}}"#);
    }

    #[test]
    fn test_parse_error_response_maps_index_not_found() {
        let err = parse_error_response(
            reqwest::StatusCode::NOT_FOUND,
            r#"{"error":{"type":"index_not_found_exception","reason":"missing","index":"foo"}}"#,
        );

        assert!(matches!(
            err,
            ElasticsearchClientError::IndexNotFound { index } if index == "foo"
        ));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
