// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "kamu-datasets";
const SCHEMA_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const DATASET_NAME: &str = "dataset_name";
    pub const ALIAS: &str = "alias";
    pub const OWNER_NAME: &str = "owner_name";
    pub const OWNER_ID: &str = "owner_id";
    pub const KIND: &str = "kind";
    pub const CREATED_AT: &str = "created_at";
    pub const REF_CHANGED_AT: &str = "ref_changed_at";
    pub const SCHEMA_FIELDS: &str = "schema_fields";
    pub const DESCRIPTION: &str = "description";
    pub const KEYWORDS: &str = "keywords";
    pub const ATTACHMENTS: &str = "attachments";

    pub mod values {
        pub const KIND_ROOT: &str = "root";
        pub const KIND_DERIVATIVE: &str = "derivative";
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    SearchSchemaField {
        path: fields::DATASET_NAME,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: fields::ALIAS,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: false,
            enable_inner_ngrams: false,
        },
    },
    SearchSchemaField {
        path: fields::OWNER_NAME,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: fields::OWNER_ID,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: fields::KIND,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: fields::CREATED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    SearchSchemaField {
        path: fields::REF_CHANGED_AT,
        role: SearchSchemaFieldRole::DateTime,
    },
    SearchSchemaField {
        path: fields::SCHEMA_FIELDS,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: true,
            enable_inner_ngrams: false,
        },
    },
    SearchSchemaField {
        path: fields::DESCRIPTION,
        role: SearchSchemaFieldRole::Description { add_keyword: false },
    },
    SearchSchemaField {
        path: fields::KEYWORDS,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: false,
        },
    },
    SearchSchemaField {
        path: fields::ATTACHMENTS,
        role: SearchSchemaFieldRole::Prose,
    },
];

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    fields: SCHEMA_FIELDS,
    title_field: fields::ALIAS,
    upgrade_mode: SearchEntitySchemaUpgradeMode::BreakingRecreate,
    flags: SearchEntitySchemaFlags {
        enable_banning: false,
        enable_security: true,
        enable_embeddings: true,
    },
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
