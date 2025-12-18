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

pub const SCHEMA_NAME: &str = "kamu-accounts";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_ACCOUNT_NAME: &str = "account_name";
pub const FIELD_DISPLAY_NAME: &str = "display_name";
pub const FIELD_CREATED_AT: &str = "created_at";
pub const FIELD_UPDATED_AT: &str = "updated_at";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[FullTextSchemaField] = &[
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
        role: FullTextSchemaFieldRole::Name,
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

pub const SCHEMA: FullTextSearchEntitySchema = FullTextSearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    fields: SCHEMA_FIELDS,
    title_field: FIELD_ACCOUNT_NAME,
    enable_banning: true,
    upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
