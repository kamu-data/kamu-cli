// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::*;

use crate::search::schemas::molecule_search_schema_common as molecule_schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "molecule-projects";
const SCHEMA_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const IPNFT_SYMBOL: &str = "ipnft_symbol";
    pub const PROJECT_ACCOUNT_ID: &str = "project_account_id";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    molecule_schema::field_definitions::EVENT_TIME,
    molecule_schema::field_definitions::SYSTEM_TIME,
    molecule_schema::field_definitions::MOLECULE_ACCOUNT_ID,
    SearchSchemaField {
        path: fields::IPNFT_SYMBOL,
        role: SearchSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    molecule_schema::field_definitions::IPNFT_UID,
    SearchSchemaField {
        path: fields::PROJECT_ACCOUNT_ID,
        role: SearchSchemaFieldRole::Keyword,
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: SearchEntitySchemaUpgradeMode::BreakingRecreate,
    fields: SCHEMA_FIELDS,
    title_field: fields::IPNFT_SYMBOL,
    flags: SearchEntitySchemaFlags {
        enable_banning: true,
        enable_security: true,
        enable_embeddings: false,
    },
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
