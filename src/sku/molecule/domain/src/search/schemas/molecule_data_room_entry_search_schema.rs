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

use crate::search::schemas::molecule_search_schema_common as molecule_schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "molecule-data-room-entries";
const SCHEMA_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const DEPTH: &str = "depth";
    pub const CONTENT_TEXT: &str = "content_text";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    molecule_schema::field_definitions::EVENT_TIME,
    molecule_schema::field_definitions::SYSTEM_TIME,
    molecule_schema::field_definitions::MOLECULE_ACCOUNT_ID,
    molecule_schema::field_definitions::IPNFT_UID,
    molecule_schema::field_definitions::REF,
    SearchSchemaField {
        path: molecule_schema::fields::PATH,
        role: SearchSchemaFieldRole::Identifier {
            // Path is an identifier in this index, not a filter
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    SearchSchemaField {
        path: fields::DEPTH,
        role: SearchSchemaFieldRole::Integer,
    },
    molecule_schema::field_definitions::VERSION,
    molecule_schema::field_definitions::CONTENT_TYPE,
    molecule_schema::field_definitions::CONTENT_HASH,
    molecule_schema::field_definitions::CONTENT_LENGTH,
    SearchSchemaField {
        path: fields::CONTENT_TEXT,
        role: SearchSchemaFieldRole::Prose,
    },
    molecule_schema::field_definitions::ACCESS_LEVEL,
    molecule_schema::field_definitions::CHANGE_BY,
    molecule_schema::field_definitions::DESCRIPTION,
    molecule_schema::field_definitions::CATEGORIES,
    molecule_schema::field_definitions::TAGS,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
    fields: SCHEMA_FIELDS,
    title_field: molecule_schema::fields::PATH,
    enable_banning: false,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn unique_id_for_data_room_entry(ipnft_uid: &str, entry_path: &str) -> String {
    format!("{ipnft_uid}:{entry_path}")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
