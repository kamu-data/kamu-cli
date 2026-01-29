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

pub const SCHEMA_NAME: &str = "molecule-announcements";
const SCHEMA_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const HEADLINE: &str = "headline";
    pub const BODY: &str = "body";
    pub const ATTACHMENTS: &str = "attachments";

    pub const HEADLINE_KEYWORD: &str = "headline.keyword";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[SearchSchemaField] = &[
    molecule_schema::field_definitions::EVENT_TIME,
    molecule_schema::field_definitions::SYSTEM_TIME,
    molecule_schema::field_definitions::MOLECULE_ACCOUNT_ID,
    molecule_schema::field_definitions::IPNFT_UID,
    SearchSchemaField {
        path: fields::HEADLINE,
        role: SearchSchemaFieldRole::Description {
            // Announcement headlines are used as titles, so we need a separate keyword field
            add_keyword: true,
        },
    },
    SearchSchemaField {
        path: fields::BODY,
        role: SearchSchemaFieldRole::Prose,
    },
    SearchSchemaField {
        path: fields::ATTACHMENTS,
        role: SearchSchemaFieldRole::Keyword,
    },
    molecule_schema::field_definitions::ACCESS_LEVEL,
    molecule_schema::field_definitions::CHANGE_BY,
    molecule_schema::field_definitions::CATEGORIES,
    molecule_schema::field_definitions::TAGS,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: SearchEntitySchema = SearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: SearchEntitySchemaUpgradeMode::Reindex,
    fields: SCHEMA_FIELDS,
    title_field: fields::HEADLINE_KEYWORD,
    flags: SearchEntitySchemaFlags {
        enable_banning: false,
        enable_security: true,
        enable_embeddings: true,
    },
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
