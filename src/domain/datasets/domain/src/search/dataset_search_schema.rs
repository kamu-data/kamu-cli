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

pub const SCHEMA_NAME: &str = "kamu-datasets";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_DATASET_NAME: &str = "dataset_name";
pub const FIELD_ALIAS: &str = "alias";
pub const FIELD_OWNER_NAME: &str = "owner_name";
pub const FIELD_OWNER_ID: &str = "owner_id";
pub const FIELD_KIND: &str = "kind";
pub const FIELD_CREATED_AT: &str = "created_at";
pub const FIELD_REF_CHANGED_AT: &str = "ref_changed_at";
pub const FIELD_SCHEMA_FIELDS: &str = "schema_fields";
pub const FIELD_DESCRIPTION: &str = "description";
pub const FIELD_KEYWORDS: &str = "keywords";
pub const FIELD_ATTACHMENTS: &str = "attachments";

pub const FIELD_VALUE_KIND_ROOT: &str = "root";
pub const FIELD_VALUE_KIND_DERIVATIVE: &str = "derivative";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: FIELD_DATASET_NAME,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: FIELD_ALIAS,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: false,
            enable_inner_ngrams: false,
        },
    },
    SearchSchemaField {
        path: FIELD_OWNER_NAME,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: FIELD_OWNER_ID,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: FIELD_KIND,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: FIELD_CREATED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    SearchSchemaField {
        path: FIELD_REF_CHANGED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    SearchSchemaField {
        path: FIELD_SCHEMA_FIELDS,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: FIELD_DESCRIPTION,
        role: SearchSchemaFieldRole::Prose {
            enable_positions: false, // short prose
        },
    },
    SearchSchemaField {
        path: FIELD_KEYWORDS,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: FIELD_ATTACHMENTS,
        role: SearchSchemaFieldRole::Prose {
            enable_positions: true, // long prose
        },
    },
];

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    fields: SCHEMA_FIELDS,
    title_field: FIELD_ALIAS,
    enable_banning: false, // Potentially might be useful for datasets
    upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
