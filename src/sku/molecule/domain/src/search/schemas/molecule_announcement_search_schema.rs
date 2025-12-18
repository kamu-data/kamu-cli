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

use crate::search::schemas::molecule_search_schema_common as molecule_schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA_NAME: &str = "molecule-announcements";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod fields {
    pub const HEADLINE: &str = "headline";
    pub const BODY: &str = "body";
    pub const ATTACHMENTS: &str = "attachments";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[FullTextSchemaField] = &[
    molecule_schema::field_definitions::EVENT_TIME,
    molecule_schema::field_definitions::SYSTEM_TIME,
    molecule_schema::field_definitions::MOLECULE_ACCOUNT_ID,
    molecule_schema::field_definitions::IPNFT_UID,
    FullTextSchemaField {
        path: fields::HEADLINE,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: false,
        },
    },
    FullTextSchemaField {
        path: fields::BODY,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: true,
        },
    },
    FullTextSchemaField {
        path: fields::ATTACHMENTS,
        role: FullTextSchemaFieldRole::Keyword,
    },
    molecule_schema::field_definitions::ACCESS_LEVEL,
    molecule_schema::field_definitions::CHANGE_BY,
    molecule_schema::field_definitions::CATEGORIES,
    molecule_schema::field_definitions::TAGS,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: FullTextSearchEntitySchema = FullTextSearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    fields: SCHEMA_FIELDS,
    title_field: fields::HEADLINE,
    enable_banning: false,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
