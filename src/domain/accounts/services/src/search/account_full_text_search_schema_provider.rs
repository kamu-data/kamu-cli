// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct AccountFullTextSearchSchemaProvider {
    expensive_account_repo: Arc<dyn kamu_accounts::ExpensiveAccountRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu_search::FullTextSearchEntitySchemaProvider for AccountFullTextSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.accounts.AccountFullTextSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::FullTextSearchEntitySchema] {
        &[ACCOUNT_FULL_TEXT_SEARCH_ENTITY_SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        repo: &dyn FullTextSearchRepository,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.entity_kind == FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT);

        const CHUNK_SIZE: usize = 500;

        use kamu_accounts::ExpensiveAccountRepositoryExt;
        let mut accounts_stream = self.expensive_account_repo.all_accounts();

        let mut account_documents = Vec::new();
        let mut total_indexed = 0;

        use futures::TryStreamExt;
        while let Some(account) = accounts_stream.try_next().await? {
            let account_document = serde_json::json!({
                FIELD_ACCOUNT_NAME: account.account_name.to_string(),
                FIELD_DISPLAY_NAME: account.display_name,
                FIELD_CREATED_AT: account.registered_at.to_rfc3339(),
            });
            account_documents.push((account.id.to_string(), account_document));

            // Index in chunks to avoid memory overwhelming
            if account_documents.len() >= CHUNK_SIZE {
                repo.index_bulk(FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT, account_documents)
                    .await?;
                total_indexed += CHUNK_SIZE;
                account_documents = Vec::new();
            }
        }

        // Index remaining documents
        if !account_documents.is_empty() {
            let remaining_count = account_documents.len();
            repo.index_bulk(FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT, account_documents)
                .await?;
            total_indexed += remaining_count;
        }

        Ok(total_indexed)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT: &str = "kamu-accounts";
const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FIELD_ACCOUNT_NAME: &str = "account_name";
const FIELD_DISPLAY_NAME: &str = "display_name";
const FIELD_CREATED_AT: &str = "created_at";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_ACCOUNT_NAME,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_DISPLAY_NAME,
        role: FullTextSchemaFieldRole::Prose {
            // TODO: tune this, not really prose
            enable_positions: false,
        },
    },
    FullTextSchemaField {
        path: FIELD_CREATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
];

const ACCOUNT_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION,
        fields: ACCOUNT_FIELDS,
        upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
