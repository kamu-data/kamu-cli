// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{
    SearchEntitySchema,
    SearchEntitySchemaUpgradeMode,
    SearchSchemaField,
    SearchSchemaFieldRole,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "kamu-accounts";
const SCHEMA_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const ACCOUNT_NAME: &str = "account_name";
    pub const DISPLAY_NAME: &str = "display_name";
    pub const CREATED_AT: &str = "created_at";
    pub const UPDATED_AT: &str = "updated_at";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: fields::ACCOUNT_NAME,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: fields::DISPLAY_NAME,
        role: SearchSchemaFieldRole::Name,
    },
    SearchSchemaField {
        path: fields::CREATED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    SearchSchemaField {
        path: fields::UPDATED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    kamu_search::field_definitions::VISIBILITY,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    fields: SCHEMA_FIELDS,
    title_field: fields::ACCOUNT_NAME,
    enable_banning: true,
    upgrade_mode: SearchEntitySchemaUpgradeMode::BreakingRecreate,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
