// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::domain::{
    MoleculeDataRoomEntry,
    MoleculeProject,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Projects
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_project_from_entity(project: &MoleculeProject) -> serde_json::Value {
    serde_json::json!({
        project_schema::FIELD_CREATED_AT: project.system_time,
        project_schema::FIELD_UPDATED_AT: project.system_time,
        project_schema::FIELD_IPNFT_SYMBOL: project.ipnft_symbol,
        project_schema::FIELD_IPNFT_UID: project.ipnft_uid,
        project_schema::FIELD_ACCOUNT_ID: project.account_id,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_project_from_parts(
    ipnft_uid: &str,
    ipnft_symbol: &str,
    account_id: &odf::AccountID,
    system_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        project_schema::FIELD_CREATED_AT: system_time,
        project_schema::FIELD_UPDATED_AT: system_time,
        project_schema::FIELD_IPNFT_SYMBOL: ipnft_symbol,
        project_schema::FIELD_IPNFT_UID: ipnft_uid,
        project_schema::FIELD_ACCOUNT_ID: account_id,
        kamu_search::FULL_TEXT_SEARCH_FIELD_IS_BANNED: false,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_project_when_ban_status_changed(
    is_banned: bool,
    system_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        project_schema::FIELD_UPDATED_AT: system_time,
        "is_banned": is_banned,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Data Room Entries
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_data_room_entry_from_entity(
    project: &MoleculeProject,
    entry: &MoleculeDataRoomEntry,
) -> serde_json::Value {
    serde_json::json!({
        data_room_entry_schema::FIELD_CREATED_AT: entry.system_time,
        data_room_entry_schema::FIELD_UPDATED_AT: entry.system_time,
        data_room_entry_schema::FIELD_IPNFT_UID: project.ipnft_uid,
        data_room_entry_schema::FIELD_REFERENCE: entry.reference,
        data_room_entry_schema::FIELD_PATH: entry.path,
        data_room_entry_schema::FIELD_VERSION: entry.denormalized_latest_file_info.version,
        data_room_entry_schema::FIELD_ACCESS_LEVEL: entry.denormalized_latest_file_info.access_level,
        data_room_entry_schema::FIELD_DESCRIPTION: entry.denormalized_latest_file_info.description,
        data_room_entry_schema::FIELD_CATEGORIES: entry.denormalized_latest_file_info.categories,
        data_room_entry_schema::FIELD_TAGS: entry.denormalized_latest_file_info.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
