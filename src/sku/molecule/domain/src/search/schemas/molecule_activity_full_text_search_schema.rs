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

pub const SCHEMA_NAME: &str = "molecule-activity-entries";
const SCHEMA_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FIELD_EVENT_TIME: &str = "event_time";
pub const FIELD_SYSTEM_TIME: &str = "system_time";
pub const FIELD_OFFSET: &str = "offset";
pub const FIELD_MOLECULE_ACCOUNT_ID: &str = "molecule_account_id";
pub const FIELD_IPNFT_UID: &str = "ipnft_uid";
pub const FIELD_ACTIVITY_TYPE: &str = "activity_type";
pub const FIELD_PATH: &str = "path";
pub const FIELD_REF: &str = "ref";
pub const FIELD_VERSION: &str = "version";
pub const FIELD_CONTENT_TYPE: &str = "content_type";
pub const FIELD_CONTENT_HASH: &str = "content_hash";
pub const FIELD_CONTENT_LENGTH: &str = "content_length";
pub const FIELD_ACCESS_LEVEL: &str = "molecule_access_level";
pub const FIELD_CHANGE_BY: &str = "molecule_change_by";
pub const FIELD_DESCRIPTION: &str = "description";
pub const FIELD_TAGS: &str = "tags";
pub const FIELD_CATEGORIES: &str = "categories";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[kamu_search::FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_EVENT_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_SYSTEM_TIME,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_OFFSET,
        role: FullTextSchemaFieldRole::Integer,
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
        path: FIELD_ACTIVITY_TYPE,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_PATH,
        role: FullTextSchemaFieldRole::Keyword, // Not identifier in this index, filters only
    },
    FullTextSchemaField {
        path: FIELD_REF,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_VERSION,
        role: FullTextSchemaFieldRole::Integer,
    },
    FullTextSchemaField {
        path: FIELD_CONTENT_TYPE,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_CONTENT_HASH,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_CONTENT_LENGTH,
        role: FullTextSchemaFieldRole::Integer,
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
        path: FIELD_DESCRIPTION,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: true,
        },
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
