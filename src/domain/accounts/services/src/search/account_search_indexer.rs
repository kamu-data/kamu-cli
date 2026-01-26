// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_accounts::{AccountDisplayName, ExpensiveAccountRepository, account_search_schema};
use kamu_search::{SearchIndexUpdateOperation, SearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_account(account: &kamu_accounts::Account) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::fields::ACCOUNT_NAME: account.account_name.to_string(),
        account_search_schema::fields::DISPLAY_NAME: account.display_name,
        account_search_schema::fields::CREATED_AT: account.registered_at.to_rfc3339(),
        account_search_schema::fields::UPDATED_AT: account.registered_at.to_rfc3339(), // Note: we have no marker in DB
        kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PUBLIC_GUEST,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_parts(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    registered_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::fields::ACCOUNT_NAME: account_name,
        account_search_schema::fields::DISPLAY_NAME: display_name,
        account_search_schema::fields::CREATED_AT: registered_at.to_rfc3339(),
        account_search_schema::fields::UPDATED_AT: registered_at.to_rfc3339(), // Starting value
        kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PUBLIC_GUEST,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_account(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    updated_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::fields::ACCOUNT_NAME: account_name,
        account_search_schema::fields::DISPLAY_NAME: display_name,
        account_search_schema::fields::UPDATED_AT: updated_at.to_rfc3339(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_accounts(
    expensive_account_repo: &dyn ExpensiveAccountRepository,
    repo: &dyn SearchRepository,
) -> Result<usize, InternalError> {
    use kamu_accounts::ExpensiveAccountRepositoryExt;

    const BULK_SIZE: usize = 500;

    let mut accounts_stream = expensive_account_repo.all_accounts();

    let mut operations = Vec::new();
    let mut total_indexed = 0;

    use futures::TryStreamExt;
    while let Some(account) = accounts_stream.try_next().await? {
        // Prepare document
        let account_document = index_from_account(&account);
        operations.push(SearchIndexUpdateOperation::Index {
            id: account.id.to_string(),
            doc: account_document,
        });

        // Index in chunks to avoid memory overwhelming
        if operations.len() >= BULK_SIZE {
            repo.bulk_update(account_search_schema::SCHEMA_NAME, operations)
                .await?;
            total_indexed += BULK_SIZE;
            operations = Vec::new();
        }
    }

    // Index remaining documents
    if !operations.is_empty() {
        let remaining_count = operations.len();
        repo.bulk_update(account_search_schema::SCHEMA_NAME, operations)
            .await?;
        total_indexed += remaining_count;
    }

    Ok(total_indexed)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
