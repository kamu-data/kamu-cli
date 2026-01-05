// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use internal_error::InternalError;
use kamu_accounts::account_search_schema;
use kamu_search::*;

use crate::search::account_search_indexer::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::SearchEntitySchemaProvider)]
pub struct AccountSearchSchemaProvider {
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AccountSearchSchemaProvider {
    #[transactional_method1(expensive_account_repo: Arc<dyn kamu_accounts::ExpensiveAccountRepository>)]
    async fn index_accounts(
        &self,
        search_repo: Arc<dyn SearchRepository>,
    ) -> Result<usize, InternalError> {
        index_accounts(expensive_account_repo.as_ref(), search_repo.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu_search::SearchEntitySchemaProvider for AccountSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.accounts.AccountSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::SearchEntitySchema] {
        &[account_search_schema::SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        search_repo: Arc<dyn SearchRepository>,
        schema: &SearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.schema_name == account_search_schema::SCHEMA_NAME);
        self.index_accounts(search_repo).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
