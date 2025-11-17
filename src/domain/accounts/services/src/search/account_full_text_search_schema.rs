// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_accounts::AccountDisplayName;
use kamu_search::{
    FullTextSchemaField,
    FullTextSchemaFieldRole,
    FullTextSearchEntitySchema,
    FullTextSearchEntitySchemaUpgradeMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT: &str = "kamu-accounts";
pub(crate) const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const FIELD_ACCOUNT_NAME: &str = "account_name";
pub(crate) const FIELD_DISPLAY_NAME: &str = "display_name";
pub(crate) const FIELD_CREATED_AT: &str = "created_at";
pub(crate) const FIELD_UPDATED_AT: &str = "updated_at";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_ACCOUNT_NAME,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_DISPLAY_NAME,
        role: FullTextSchemaFieldRole::Title,
    },
    FullTextSchemaField {
        path: FIELD_CREATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_UPDATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const ACCOUNT_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION,
        fields: ACCOUNT_FIELDS,
        upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_account(account: &kamu_accounts::Account) -> serde_json::Value {
    serde_json::json!({
        FIELD_ACCOUNT_NAME: account.account_name.to_string(),
        FIELD_DISPLAY_NAME: account.display_name,
        FIELD_CREATED_AT: account.registered_at.to_rfc3339(),
        FIELD_UPDATED_AT: account.registered_at.to_rfc3339(), // Note: we have no marker in DB
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_from_parts(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    registered_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        FIELD_ACCOUNT_NAME: account_name,
        FIELD_DISPLAY_NAME: display_name,
        FIELD_CREATED_AT: registered_at.to_rfc3339(),
        FIELD_UPDATED_AT: registered_at.to_rfc3339(), // Starting value
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_account(
    account_name: &odf::AccountName,
    display_name: &AccountDisplayName,
    updated_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        FIELD_ACCOUNT_NAME: account_name,
        FIELD_DISPLAY_NAME: display_name,
        FIELD_UPDATED_AT: updated_at.to_rfc3339(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
