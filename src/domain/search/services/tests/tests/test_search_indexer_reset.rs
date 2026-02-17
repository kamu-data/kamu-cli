// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use internal_error::InternalError;
use kamu_search::*;
use kamu_search_services::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_search_indices_resets_all_when_entity_names_empty() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let repo = Arc::new(TestSearchRepository::new(events.clone()));
    let provider = Arc::new(TestSchemaProvider::new(events.clone()));

    let indexer = SearchIndexerImpl::new(
        Arc::new(SearchIndexerConfig::default()),
        repo,
        vec![provider],
    );

    indexer.reset_search_indices(vec![]).await.unwrap();

    let got = events.lock().unwrap().clone();
    let expected = vec![
        "lock:test-a",
        "drop:test-a",
        "ensure:test-a",
        "index:test-a",
        "unlock:test-a",
        "lock:test-b",
        "drop:test-b",
        "ensure:test-b",
        "index:test-b",
        "unlock:test-b",
    ];
    assert_eq!(got, expected);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_search_indices_resets_only_requested_entity_name() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let repo = Arc::new(TestSearchRepository::new(events.clone()));
    let provider = Arc::new(TestSchemaProvider::new(events.clone()));

    let indexer = SearchIndexerImpl::new(
        Arc::new(SearchIndexerConfig::default()),
        repo,
        vec![provider],
    );

    indexer
        .reset_search_indices(vec!["test-b".to_string()])
        .await
        .unwrap();

    let got = events.lock().unwrap().clone();
    let expected = vec![
        "lock:test-b",
        "drop:test-b",
        "ensure:test-b",
        "index:test-b",
        "unlock:test-b",
    ];
    assert_eq!(got, expected);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_search_indices_rejects_unknown_entity_name() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let repo = Arc::new(TestSearchRepository::new(events.clone()));
    let provider = Arc::new(TestSchemaProvider::new(events.clone()));

    let indexer = SearchIndexerImpl::new(
        Arc::new(SearchIndexerConfig::default()),
        repo,
        vec![provider],
    );

    let err = indexer
        .reset_search_indices(vec!["unknown".to_string()])
        .await
        .unwrap_err();
    assert!(
        err.reason()
            .contains("Unknown search entity name(s): unknown"),
        "{err}"
    );

    assert!(events.lock().unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_FIELDS: [SearchSchemaField; 1] = [SearchSchemaField {
    path: "id",
    role: SearchSchemaFieldRole::Identifier {
        hierarchical: false,
        enable_edge_ngrams: false,
        enable_inner_ngrams: false,
    },
}];

const TEST_SCHEMA_A: SearchEntitySchema = SearchEntitySchema {
    schema_name: "test-a",
    version: 1,
    upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
    fields: &TEST_FIELDS,
    title_field: "id",
    flags: SearchEntitySchemaFlags {
        enable_banning: false,
        enable_security: false,
        enable_embeddings: false,
    },
};

const TEST_SCHEMA_B: SearchEntitySchema = SearchEntitySchema {
    schema_name: "test-b",
    version: 1,
    upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
    fields: &TEST_FIELDS,
    title_field: "id",
    flags: SearchEntitySchemaFlags {
        enable_banning: false,
        enable_security: false,
        enable_embeddings: false,
    },
};

const TEST_SCHEMAS: [SearchEntitySchema; 2] = [TEST_SCHEMA_A, TEST_SCHEMA_B];

struct TestSchemaProvider {
    events: Arc<Mutex<Vec<String>>>,
}

impl TestSchemaProvider {
    fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
        Self { events }
    }
}

#[async_trait::async_trait]
impl SearchEntitySchemaProvider for TestSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "test-provider"
    }

    fn provide_schemas(&self) -> &[SearchEntitySchema] {
        &TEST_SCHEMAS
    }

    async fn run_schema_initial_indexing(
        &self,
        _search_repo: Arc<dyn SearchRepository>,
        schema: &SearchEntitySchema,
    ) -> Result<usize, InternalError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("index:{}", schema.schema_name));
        Ok(1)
    }
}

struct TestSearchRepository {
    events: Arc<Mutex<Vec<String>>>,
}

impl TestSearchRepository {
    fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
        Self { events }
    }
}

#[async_trait::async_trait]
impl SearchRepository for TestSearchRepository {
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        unimplemented!()
    }

    async fn ensure_entity_index(
        &self,
        schema: &SearchEntitySchema,
    ) -> Result<(), SearchEnsureEntityIndexError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("ensure:{}", schema.schema_name));
        Ok(())
    }

    async fn total_documents(&self) -> Result<u64, InternalError> {
        unimplemented!()
    }

    async fn documents_of_kind(
        &self,
        _schema_name: SearchEntitySchemaName,
    ) -> Result<u64, InternalError> {
        unimplemented!()
    }

    async fn find_document_by_id(
        &self,
        _schema_name: SearchEntitySchemaName,
        _id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        unimplemented!()
    }

    async fn bulk_update(
        &self,
        _schema_name: SearchEntitySchemaName,
        _operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn lock_schema(&self, schema_name: SearchEntitySchemaName) -> Result<(), InternalError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("lock:{schema_name}"));
        Ok(())
    }

    async fn unlock_schema(
        &self,
        schema_name: SearchEntitySchemaName,
    ) -> Result<(), InternalError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("unlock:{schema_name}"));
        Ok(())
    }

    async fn drop_schemas(
        &self,
        schema_names: &[SearchEntitySchemaName],
    ) -> Result<(), InternalError> {
        for schema_name in schema_names {
            self.events
                .lock()
                .unwrap()
                .push(format!("drop:{schema_name}"));
        }
        Ok(())
    }

    async fn drop_all_schemas(&self) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn listing_search(
        &self,
        _security_ctx: SearchSecurityContext,
        _req: ListingSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        unimplemented!()
    }

    async fn text_search(
        &self,
        _security_ctx: SearchSecurityContext,
        _req: TextSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        unimplemented!()
    }

    async fn vector_search(
        &self,
        _security_ctx: SearchSecurityContext,
        _req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        unimplemented!()
    }

    async fn hybrid_search(
        &self,
        _security_ctx: SearchSecurityContext,
        _req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
