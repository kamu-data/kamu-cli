// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{
    FullTextSchemaField,
    FullTextSchemaFieldRole,
    FullTextSearchEntitySchema,
    FullTextSearchEntitySchemaUpgradeMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "molecule-projects";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_EVENT_TIME: &str = "event_time";
pub const FIELD_SYSTEM_TIME: &str = "system_time";
pub const FIELD_MOLECULE_ACCOUNT_ID: &str = "molecule_account_id";
pub const FIELD_IPNFT_SYMBOL: &str = "ipnft_symbol";
pub const FIELD_IPNFT_UID: &str = "ipnft_uid";
pub const FIELD_PROJECT_ACCOUNT_ID: &str = "project_account_id";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_EVENT_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_SYSTEM_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_MOLECULE_ACCOUNT_ID,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_IPNFT_SYMBOL,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_IPNFT_UID,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_PROJECT_ACCOUNT_ID,
        role: FullTextSchemaFieldRole::Keyword,
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: FullTextSearchEntitySchema = FullTextSearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    fields: SCHEMA_FIELDS,
    title_field: FIELD_IPNFT_SYMBOL,
    enable_banning: true,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
