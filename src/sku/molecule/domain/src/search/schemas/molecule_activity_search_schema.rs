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

pub const SCHEMA_NAME: &str = "molecule-activity-entries";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const ACTIVITY_TYPE: &str = "activity_type";
    pub const OFFSET: &str = "offset";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[kamu_search::SearchSchemaField] = &[
    molecule_schema::field_definitions::EVENT_TIME,
    molecule_schema::field_definitions::SYSTEM_TIME,
    SearchSchemaField {
        path: fields::OFFSET,
        role: SearchSchemaFieldRole::Integer,
    },
    molecule_schema::field_definitions::MOLECULE_ACCOUNT_ID,
    molecule_schema::field_definitions::IPNFT_UID,
    SearchSchemaField {
        path: fields::ACTIVITY_TYPE,
        role: SearchSchemaFieldRole::Keyword,
    },
    SearchSchemaField {
        path: molecule_schema::fields::PATH,
        role: SearchSchemaFieldRole::Keyword, // Not identifier in this index, filters only
    },
    molecule_schema::field_definitions::REF,
    molecule_schema::field_definitions::VERSION,
    molecule_schema::field_definitions::CONTENT_TYPE,
    molecule_schema::field_definitions::CONTENT_HASH,
    molecule_schema::field_definitions::CONTENT_LENGTH,
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
    title_field: "_id",
    enable_banning: false,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn unique_id_for_data_room_activity(
    molecule_account_id: &odf::AccountID,
    offset: u64,
) -> String {
    format!("{molecule_account_id}:{offset}")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
