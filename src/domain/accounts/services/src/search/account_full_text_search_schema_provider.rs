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
use kamu_accounts::account_full_text_search_schema as account_schema;
use kamu_search::*;

use super::account_full_text_search_schema_helpers::*;

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
        &[account_schema::SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        repo: &dyn FullTextSearchRepository,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.schema_name == account_schema::SCHEMA_NAME);

        // Index accounts in chunks

        const CHUNK_SIZE: usize = 500;

        use kamu_accounts::ExpensiveAccountRepositoryExt;
        let mut accounts_stream = self.expensive_account_repo.all_accounts();

        let mut account_documents = Vec::new();
        let mut total_indexed = 0;

        use futures::TryStreamExt;
        while let Some(account) = accounts_stream.try_next().await? {
            // Prepare document
            let account_document = index_from_account(&account);
            account_documents.push((account.id.to_string(), account_document));

            // Index in chunks to avoid memory overwhelming
            if account_documents.len() >= CHUNK_SIZE {
                repo.index_bulk(account_schema::SCHEMA_NAME, account_documents)
                    .await?;
                total_indexed += CHUNK_SIZE;
                account_documents = Vec::new();
            }
        }

        // Index remaining documents
        if !account_documents.is_empty() {
            let remaining_count = account_documents.len();
            repo.index_bulk(account_schema::SCHEMA_NAME, account_documents)
                .await?;
            total_indexed += remaining_count;
        }

        Ok(total_indexed)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
