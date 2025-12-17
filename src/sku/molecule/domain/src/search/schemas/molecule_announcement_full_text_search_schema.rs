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

pub const SCHEMA_NAME: &str = "molecule-announcements";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_EVENT_TIME: &str = "event_time";
pub const FIELD_SYSTEM_TIME: &str = "system_time";
pub const FIELD_MOLECULE_ACCOUNT_ID: &str = "molecule_account_id";
pub const FIELD_IPNFT_UID: &str = "ipnft_uid";
pub const FIELD_HEADLINE: &str = "headline";
pub const FIELD_BODY: &str = "body";
pub const FIELD_ATTACHMENTS: &str = "attachments";
pub const FIELD_ACCESS_LEVEL: &str = "molecule_access_level";
pub const FIELD_CHANGE_BY: &str = "molecule_change_by";
pub const FIELD_CATEGORIES: &str = "categories";
pub const FIELD_TAGS: &str = "tags";

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
        path: FIELD_IPNFT_UID,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_HEADLINE,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: false,
        },
    },
    FullTextSchemaField {
        path: FIELD_BODY,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_ATTACHMENTS,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_ACCESS_LEVEL,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_CHANGE_BY,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_CATEGORIES,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_TAGS,
        role: FullTextSchemaFieldRole::Keyword,
    },
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SCHEMA: FullTextSearchEntitySchema = FullTextSearchEntitySchema {
    schema_name: SCHEMA_NAME,
    version: SCHEMA_VERSION,
    upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    fields: SCHEMA_FIELDS,
    title_field: FIELD_HEADLINE,
    enable_banning: false,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
