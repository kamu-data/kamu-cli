// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_accounts::{AccountDisplayName, account_search_schema};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_account(account: &kamu_accounts::Account) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::FIELD_ACCOUNT_NAME: account.account_name.to_string(),
        account_search_schema::FIELD_DISPLAY_NAME: account.display_name,
        account_search_schema::FIELD_CREATED_AT: account.registered_at.to_rfc3339(),
        account_search_schema::FIELD_UPDATED_AT: account.registered_at.to_rfc3339(), // Note: we have no marker in DB
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_parts(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    registered_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::FIELD_ACCOUNT_NAME: account_name,
        account_search_schema::FIELD_DISPLAY_NAME: display_name,
        account_search_schema::FIELD_CREATED_AT: registered_at.to_rfc3339(),
        account_search_schema::FIELD_UPDATED_AT: registered_at.to_rfc3339(), // Starting value
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_account(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    updated_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        account_search_schema::FIELD_ACCOUNT_NAME: account_name,
        account_search_schema::FIELD_DISPLAY_NAME: display_name,
        account_search_schema::FIELD_UPDATED_AT: updated_at.to_rfc3339(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
