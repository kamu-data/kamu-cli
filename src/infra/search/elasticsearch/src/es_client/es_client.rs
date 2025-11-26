// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::ElasticSearchFullTextSearchConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_ELASTICSEARCH_USER: &str = "elastic";
const DEFAULT_ELASTICSEARCH_PASSWORD: &str = "root";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchClient {
    client: elasticsearch::Elasticsearch,
    _config: ElasticSearchFullTextSearchConfig,
}

#[common_macros::method_names_consts]
impl ElasticSearchClient {
    #[tracing::instrument(level = "info", name = ElasticSearchClient_init, skip_all)]
    pub fn init(
        config: ElasticSearchFullTextSearchConfig,
    ) -> Result<Self, ElasticSearchClientBuildError> {
        use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};

        let pool = SingleNodeConnectionPool::new(config.url.clone());

        let builder = TransportBuilder::new(pool)
            .disable_proxy() // often desirable in k8s
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .request_body_compression(config.enable_compression)
            .auth(elasticsearch::auth::Credentials::Basic(
                DEFAULT_ELASTICSEARCH_USER.into(),
                config
                    .password
                    .as_ref()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_else(|| DEFAULT_ELASTICSEARCH_PASSWORD.into()),
            ));

        let transport = builder.build()?;
        let client = elasticsearch::Elasticsearch::new(transport);
        Ok(Self {
            client,
            _config: config,
        })
    }

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_cluster_health, skip_all)]
    pub async fn cluster_health(&self) -> Result<serde_json::Value, ElasticSearchClientError> {
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

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_total_documents, skip_all)]
    pub async fn total_documents(&self) -> Result<u64, ElasticSearchClientError> {
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
        name = ElasticSearchClient_documents_in_index,
        skip_all,
        fields(index_name)
    )]
    pub async fn documents_in_index(
        &self,
        index_name: &str,
    ) -> Result<u64, ElasticSearchClientError> {
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

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_search, skip_all)]
    pub async fn search(
        &self,
        req_body: serde_json::Value,
        index_names: &[&str],
    ) -> Result<es_client::SearchResponse, ElasticSearchClientError> {
        tracing::debug!(index_names = ?index_names, req_body = ?req_body, "Executing ElasticSearch search");
        /*println!(
            "\nES Search request: {}, indexes: {index_names:?}\n",
            serde_json::to_string_pretty(&req_body).unwrap()
        );*/

        // Send request to ElasticSearch
        let response = self
            .client
            .search(elasticsearch::SearchParts::Index(index_names))
            .body(req_body)
            .send()
            .await?;

        // Obtain and log a raw response
        let response = ensure_client_response(response).await?;
        let body: serde_json::Value = response.json().await?;
        tracing::debug!(body = ?body, "Raw ElasticSearch response");
        /*println!(
            "\nRaw ES Search response: {}\n",
            serde_json::to_string_pretty(&body).unwrap()
        );*/

        // Try interpreting the response

        let search_response: SearchResponse = serde_json::from_value(body).unwrap();
        tracing::debug!(search_response=?search_response, "Parsed search response");
        Ok(search_response)
    }

    #[tracing::instrument(level = "info", name = ElasticSearchClient_create_index, skip_all, fields(index_name))]
    pub async fn create_index(
        &self,
        index_name: &str,
        body: serde_json::Value,
    ) -> Result<(), ElasticSearchClientError> {
        tracing::debug!(index_name, body = ?body, "Creating ElasticSearch index");

        let response = self
            .client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                index_name,
            ))
            .body(body)
            .send()
            .await?;

        let _ = ensure_client_response(response).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_read_index_meta, skip_all, fields(index_name))]
    pub async fn read_index_meta(
        &self,
        index_name: &str,
    ) -> Result<serde_json::Value, ElasticSearchClientError> {
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

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_assign_alias, skip_all, fields(alias_name, to_index))]
    pub async fn assign_alias(
        &self,
        alias_name: &str,
        to_index: &str,
        maybe_filter_json: Option<serde_json::Value>,
    ) -> Result<(), ElasticSearchClientError> {
        let mut add_command_json = serde_json::json!({
            "index": to_index,
            "alias": alias_name,
            "is_write_index": true,
        });
        if let Some(filter_json) = maybe_filter_json {
            add_command_json["filter"] = filter_json;
        }

        let body = serde_json::json!({
            "actions": [
                { "remove": { "index": "*", "alias": alias_name } },
                { "add": add_command_json }
            ]
        });

        tracing::debug!(
            alias_name,
            to_index,
            body = ?body,
            "Moving ElasticSearch alias to new index"
        );

        let response = self
            .client
            .indices()
            .update_aliases()
            .body(body)
            .send()
            .await?;

        let _ = ensure_client_response(response).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_resolve_alias_indices, skip_all, fields(alias_name))]
    pub async fn resolve_alias_indices(
        &self,
        alias_name: &str,
    ) -> Result<Vec<String>, ElasticSearchClientError> {
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

    #[tracing::instrument(level = "debug", name = ElasticSearchClient_reindex_data, skip_all, fields(from_index, to_index))]
    pub async fn reindex_data(
        &self,
        from_index: &str,
        to_index: &str,
    ) -> Result<(), ElasticSearchReindexError> {
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
            "Reindexing data from one ElasticSearch index to another"
        );

        let response = self
            .client
            .reindex()
            .body(body)
            .wait_for_completion(true) // block until done for now
            .refresh(true) // refresh dest so new alias sees all docs
            .send()
            .await
            .map_err(|e| ElasticSearchReindexError::Client(e.into()))?;

        let response = ensure_client_response(response)
            .await
            .map_err(ElasticSearchReindexError::Client)?;

        let value: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ElasticSearchReindexError::Client(e.into()))?;

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
            return Err(ElasticSearchReindexError::ReindexFailures(summary.failures));
        }

        // We only expect new documents to be created, no updates or deletions
        if summary.deleted > 0 || summary.updated > 0 {
            return Err(ElasticSearchReindexError::UnexpectedDocumentChanges {
                updated: summary.updated,
                deleted: summary.deleted,
            });
        }

        // Success
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        name = ElasticSearchClient_bulk_index,
        skip_all,
        fields(index_name, num_docs = docs.len())
    )]
    pub async fn bulk_index(
        &self,
        index_name: &str,
        docs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), ElasticSearchClientError> {
        use elasticsearch::BulkParts;

        if docs.is_empty() {
            return Ok(());
        }

        let mut body: Vec<elasticsearch::BulkOperation<_>> = Vec::with_capacity(docs.len() * 2);

        for (doc_id, doc_value) in docs {
            body.push(
                elasticsearch::BulkOperation::index(doc_value)
                    .id(doc_id)
                    .into(),
            );
        }

        tracing::debug!(
            index_name,
            num_operations = body.len(),
            "Executing bulk index operation"
        );

        let response = self
            .client
            .bulk(BulkParts::Index(index_name))
            .body(body)
            .send()
            .await?;

        handle_bulk_response(response, index_name, "index").await
    }

    #[tracing::instrument(
        level = "debug",
        name = ElasticSearchClient_bulk_update,
        skip_all,
        fields(index_name, num_updates = updates.len())
    )]
    pub async fn bulk_update(
        &self,
        index_name: &str,
        updates: Vec<(String, serde_json::Value)>,
    ) -> Result<(), ElasticSearchClientError> {
        use elasticsearch::BulkParts;

        if updates.is_empty() {
            return Ok(());
        }

        let mut body: Vec<elasticsearch::BulkOperation<serde_json::Value>> =
            Vec::with_capacity(updates.len());

        for (doc_id, partial_doc) in updates {
            body.push(
                elasticsearch::BulkOperation::update(
                    doc_id,
                    serde_json::json!({"doc": partial_doc}),
                )
                .into(),
            );
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

    #[tracing::instrument(
        level = "debug",
        name = ElasticSearchClient_bulk_delete,
        skip_all,
        fields(index_name, num_ids = ids.len())
    )]
    pub async fn bulk_delete(
        &self,
        index_name: &str,
        ids: Vec<String>,
    ) -> Result<(), ElasticSearchClientError> {
        use elasticsearch::BulkParts;

        if ids.is_empty() {
            return Ok(());
        }

        let mut body: Vec<elasticsearch::BulkOperation<serde_json::Value>> =
            Vec::with_capacity(ids.len());

        for doc_id in ids {
            body.push(elasticsearch::BulkOperation::delete(doc_id).into());
        }

        tracing::debug!(
            index_name,
            num_operations = body.len(),
            "Executing bulk delete operation"
        );

        let response = self
            .client
            .bulk(BulkParts::Index(index_name))
            .body(body)
            .send()
            .await?;

        handle_bulk_response(response, index_name, "delete").await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticSearchReindexError {
    #[error("ElasticSearch client error: {0}")]
    Client(#[from] ElasticSearchClientError),

    #[error("Detected unexpected document updates or deletions during reindexing")]
    UnexpectedDocumentChanges { updated: u64, deleted: u64 },

    #[error("Reindexing failed with failures: {0:?}")]
    ReindexFailures(Vec<serde_json::Value>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticSearchClientBuildError {
    #[error("transport error building ElasticSearch client: {0}")]
    Build(#[from] elasticsearch::http::transport::BuildError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ElasticSearchClientError {
    #[error("transport error communicating to ElasticSearch: {0}")]
    Transport(#[from] elasticsearch::Error),

    #[error("ElasticSearch error {status} ({error_type}): {reason}")]
    BadResponse {
        status: elasticsearch::http::StatusCode,
        error_type: String,
        reason: String,
        body: serde_json::Value,
    },

    #[error("ElasticSearch index not found: {index}")]
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
) -> Result<(), ElasticSearchClientError> {
    let response = ensure_client_response(response).await?;
    let bulk_result: BulkResponse = response.json().await?;

    if bulk_result.errors {
        tracing::error!(
            index_name,
            items = ?bulk_result.items,
            "Bulk {operation_name} completed with errors"
        );
        return Err(ElasticSearchClientError::BadResponse {
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
) -> Result<elasticsearch::http::response::Response, ElasticSearchClientError> {
    let status = resp.status_code();
    if status.is_success() {
        return Ok(resp);
    }

    let body_text = resp.text().await.unwrap_or_default();
    // println!("ElasticSearch error response body: {body_text}" );

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

            return Err(ElasticSearchClientError::IndexNotFound { index });
        }

        return Err(ElasticSearchClientError::BadResponse {
            status,
            error_type,
            reason,
            body: body_json,
        });
    }

    // If it wasn’t JSON, still return a structured error
    Err(ElasticSearchClientError::BadResponse {
        status,
        error_type: "non_json_error".to_string(),
        reason: body_text.clone(),
        body: serde_json::Value::String(body_text),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
