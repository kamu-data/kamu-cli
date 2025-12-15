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

pub const FIELD_CREATED_AT: &str = "created_at";
pub const FIELD_UPDATED_AT: &str = "updated_at";
pub const FIELD_IPNFT_UID: &str = "ipnft_uid";
pub const FIELD_ENTRY_PATH: &str = "entry_path";
pub const FIELD_ENTRY_REF: &str = "entry_ref";
pub const FIELD_ANNOUNCEMENT_ID: &str = "announcement_id";
pub const FIELD_ACCESS_LEVEL: &str = "access_level";
pub const FIELD_CHANGE_BY: &str = "change_by";
pub const FIELD_DESCRIPTION: &str = "description";
pub const FIELD_TAGS: &str = "tags";
pub const FIELD_CATEGORIES: &str = "categories";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SCHEMA_FIELDS: &[kamu_search::FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_CREATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_UPDATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_IPNFT_UID,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_ENTRY_PATH,
        role: FullTextSchemaFieldRole::Keyword, // Not identifier in this index, filters only
    },
    FullTextSchemaField {
        path: FIELD_ENTRY_REF,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_ANNOUNCEMENT_ID,
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
    title_field: "_id", // Questionable
    enable_banning: false,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn unique_id(activity: &crate::MoleculeGlobalActivity) -> String {
    match activity {
        crate::MoleculeGlobalActivity::DataRoomActivity(dr_activity) => {
            format!("{}:{}", dr_activity.ipnft_uid, dr_activity.offset)
        }
        crate::MoleculeGlobalActivity::Announcement(announcement) => {
            announcement.announcement.announcement_id.to_string()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
