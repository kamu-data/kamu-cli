// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::SearchIndexUpdateOperation;

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
    client: elasticsearch::Elasticsearch,
}

#[common_macros::method_names_consts]
impl ElasticsearchClient {
    #[tracing::instrument(level = "info", name = ElasticsearchClient_init, skip_all)]
    pub fn init(config: &ElasticsearchClientConfig) -> Result<Self, ElasticsearchClientBuildError> {
        tracing::info!(config = ?config, "Initializing Elasticsearch client");
        use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};

        let pool = SingleNodeConnectionPool::new(config.url.clone());

        let mut builder = TransportBuilder::new(pool)
            .disable_proxy() // often desirable in k8s
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .request_body_compression(config.enable_compression);

        if let Some(password) = &config.password {
            builder = builder.auth(elasticsearch::auth::Credentials::Basic(
                DEFAULT_ELASTICSEARCH_USER.into(),
                password.clone(),
            ));
        }

        // Configure TLS certificate if provided
        if let Some(ca_cert_path) = &config.ca_cert_pem_path {
            tracing::info!(ca_cert_path = ?ca_cert_path, "Loading CA certificate for Elasticsearch client");
            let cert_pem = std::fs::read(ca_cert_path).map_err(|e| {
                ElasticsearchClientBuildError::CertificateRead {
                    path: ca_cert_path.clone(),
                    source: e,
                }
            })?;

            let cert = elasticsearch::cert::Certificate::from_pem(&cert_pem)?;
            builder =
                builder.cert_validation(elasticsearch::cert::CertificateValidation::Full(cert));
        }

        let transport = builder.build()?;
        let client = elasticsearch::Elasticsearch::new(transport);
        Ok(Self { client })
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_cluster_health, skip_all)]
    pub async fn cluster_health(&self) -> Result<serde_json::Value, ElasticsearchClientError> {
        let response = self
            .client
            .cluster()
            .health(elasticsearch::cluster::ClusterHealthParts::None)
            .send()
            .await?;

        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;
        Ok(body)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_total_documents, skip_all)]
    pub async fn total_documents(&self) -> Result<u64, ElasticsearchClientError> {
        let response = self
            .client
            .count(elasticsearch::CountParts::None)
            .send()
            .await?;

        let response = ensure_client_response(response).await?;
        let body: es_client::CountResponse = response.json().await?;
        let count = body.count;
        Ok(count)
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
            .client
            .count(elasticsearch::CountParts::Index(&[index_name]))
            .send()
            .await?;

        let response = ensure_client_response(response).await?;

        let body: CountResponse = response.json().await?;
        let count = body.count;
        Ok(count)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_search, skip_all)]
    pub async fn search(
        &self,
        req_body: serde_json::Value,
        index_names: &[&str],
    ) -> Result<es_client::SearchResponse, ElasticsearchClientError> {
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
            .client
            .search(elasticsearch::SearchParts::Index(index_names))
            .body(req_body)
            .send()
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
        let search_response: SearchResponse = serde_json::from_value(body).unwrap();
        Ok(search_response)
    }

    #[tracing::instrument(level = "debug", name = ElasticsearchClient_find_document_by_id, skip_all, fields(index_name, id))]
    pub async fn find_document_by_id(
        &self,
        index_name: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>, ElasticsearchClientError> {
        use elasticsearch::GetParts;
        let response = self
            .client
            .get(GetParts::IndexId(index_name, id))
            .send()
            .await?;

        // 404 means document does not exist
        if response.status_code().as_u16() == 404 {
            return Ok(None);
        }

        let response = ensure_client_response(response).await?;
        let body: es_client::GetDocumentByIdResponse = response.json().await?;
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
        use elasticsearch::indices::IndicesGetParts;

        let pattern = format!("{prefix}*",);
        let response = self
            .client
            .indices()
            .get(IndicesGetParts::Index(&[&pattern]))
            .send()
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
        use elasticsearch::indices::IndicesRefreshParts;

        tracing::debug!(index_names = ?index_names, "Refreshing Elasticsearch indices");

        let response = self
            .client
            .indices()
            .refresh(IndicesRefreshParts::Index(index_names))
            .send()
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
            .client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                index_name,
            ))
            .wait_for_active_shards("1")
            .body(body)
            .send()
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
        tracing::info!(index_names = ?index_names, "Deleting Elasticsearch indices");

        use elasticsearch::indices::IndicesDeleteParts;

        let response = self
            .client
            .indices()
            .delete(IndicesDeleteParts::Index(index_names))
            .send()
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
            .client
            .indices()
            .get_mapping(elasticsearch::indices::IndicesGetMappingParts::Index(&[
                index_name,
            ]))
            .send()
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
        use elasticsearch::indices::IndicesExistsAliasParts;

        // Check if alias exists
        // Removing non-existing alias may cause an error
        let alias_exists = {
            let resp = self
                .client
                .indices()
                .exists_alias(IndicesExistsAliasParts::Name(&[alias_name]))
                .send()
                .await?;
            resp.status_code() == 200
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

        // Existing alias? Remove + Add new
        let actions = if alias_exists {
            serde_json::json!([
                { "remove": { "index": "*", "alias": alias_name } },
                { "add": add_command_json }
            ])
        } else {
            // New alias? Just Add
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
            .client
            .indices()
            .update_aliases()
            .body(body)
            .send()
            .await?;

        let _ = ensure_client_response(response).await?;

        // Ensure cluster-state propagation:
        // callers can safely use alias immediately after.
        self.wait_for_alias(alias_name).await?;

        Ok(())
    }

    /// Wait until alias is visible in cluster state.
    async fn wait_for_alias(&self, alias_name: &str) -> Result<(), ElasticsearchClientError> {
        use elasticsearch::indices::IndicesExistsAliasParts;

        self.retry_transient("wait_for_alias", ES_PROPAGATION_TIMEOUT, || async {
            let resp = self
                .client
                .indices()
                .exists_alias(IndicesExistsAliasParts::Name(&[alias_name]))
                .send()
                .await?;

            match resp.status_code() {
                elasticsearch::http::StatusCode::OK => Ok(()),
                elasticsearch::http::StatusCode::NOT_FOUND => {
                    Err(ElasticsearchClientError::IndexNotFound {
                        index: alias_name.to_owned(),
                    })
                }
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
        use elasticsearch::indices::IndicesGetAliasParts;

        let response = self
            .client
            .indices()
            .get_alias(IndicesGetAliasParts::Name(&[alias_name]))
            .send()
            .await?;

        // 404 means alias does not exist
        if response.status_code().as_u16() == 404 {
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
            .client
            .reindex()
            .body(body)
            .wait_for_completion(true) // block until done for now
            .refresh(true) // refresh dest so new alias sees all docs
            .send()
            .await
            .map_err(|e| ElasticsearchReindexError::Client(e.into()))?;

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

        let summary = serde_json::from_value::<ReindexSummary>(value).unwrap();

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
        use elasticsearch::BulkParts;

        if operations.is_empty() {
            return Ok(());
        }

        let mut body: Vec<elasticsearch::BulkOperation<serde_json::Value>> =
            Vec::with_capacity(operations.len());

        for op in operations {
            match op {
                SearchIndexUpdateOperation::Index { id, doc } => {
                    body.push(elasticsearch::BulkOperation::index(doc).id(id).into());
                }
                SearchIndexUpdateOperation::Update {
                    id,
                    doc: partial_doc,
                } => {
                    body.push(
                        elasticsearch::BulkOperation::update(
                            id,
                            serde_json::json!({"doc": partial_doc}),
                        )
                        .into(),
                    );
                }
                SearchIndexUpdateOperation::Delete { id } => {
                    body.push(elasticsearch::BulkOperation::delete(id).into());
                }
            }
        }

        tracing::debug!(
            index_name,
            num_operations = body.len(),
            "Executing bulk update operation"
        );

        let response = self
            .client
            .bulk(BulkParts::Index(index_name))
            .body(body)
            .send()
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
        Fut: std::future::Future<Output = Result<T, ElasticsearchClientError>>,
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
                if *status == elasticsearch::http::StatusCode::SERVICE_UNAVAILABLE {
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
    Build(#[from] elasticsearch::http::transport::BuildError),

    #[error("failed to read CA certificate from {path}: {source}")]
    CertificateRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("invalid certificate format: {0}")]
    InvalidCertificate(#[from] elasticsearch::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticsearchClientError {
    #[error("transport error communicating to Elasticsearch: {0}")]
    Transport(#[from] elasticsearch::Error),

    #[error("Elasticsearch error {status} ({error_type}): {reason}")]
    BadResponse {
        status: elasticsearch::http::StatusCode,
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
    response: elasticsearch::http::response::Response,
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
            status: elasticsearch::http::StatusCode::from_u16(500).unwrap(),
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
    resp: elasticsearch::http::response::Response,
) -> Result<elasticsearch::http::response::Response, ElasticsearchClientError> {
    let status = resp.status_code();
    if status.is_success() {
        return Ok(resp);
    }

    let body_text = resp.text().await.unwrap_or_default();
    // println!("Elasticsearch error response body: {body_text}" );

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
    if let Ok(parsed) = serde_json::from_str::<EsErrorRoot>(&body_text) {
        let error_type = parsed.error.type_;
        let reason = parsed.error.reason;
        let body_json = serde_json::from_str::<serde_json::Value>(&body_text)
            .unwrap_or(serde_json::Value::Null);

        // Special case: index_not_found
        const INDEX_NOT_FOUND_ERROR_TYPE: &str = "index_not_found_exception";
        if error_type == INDEX_NOT_FOUND_ERROR_TYPE {
            let index = body_json["error"]["index"]
                .as_str()
                .unwrap_or_default()
                .to_string();

            return Err(ElasticsearchClientError::IndexNotFound { index });
        }

        return Err(ElasticsearchClientError::BadResponse {
            status,
            error_type,
            reason,
            body: body_json,
        });
    }

    // If it wasn’t JSON, still return a structured error
    Err(ElasticsearchClientError::BadResponse {
        status,
        error_type: "non_json_error".to_string(),
        reason: body_text.clone(),
        body: serde_json::Value::String(body_text),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
