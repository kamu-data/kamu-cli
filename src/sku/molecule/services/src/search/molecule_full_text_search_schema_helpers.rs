// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_molecule_domain::{
    MoleculeAnnouncementPayloadRecord,
    MoleculeDataRoomActivityPayloadRecord,
    MoleculeDataRoomEntry,
    MoleculeGlobalAnnouncement,
    MoleculeProject,
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
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
    ipnft_uid: &str,
    entry: &MoleculeDataRoomEntry,
) -> serde_json::Value {
    serde_json::json!({
        data_room_entry_schema::FIELD_CREATED_AT: entry.system_time,
        data_room_entry_schema::FIELD_UPDATED_AT: entry.system_time,
        data_room_entry_schema::FIELD_IPNFT_UID: ipnft_uid,
        data_room_entry_schema::FIELD_REFERENCE: entry.reference,
        data_room_entry_schema::FIELD_PATH: entry.path,
        data_room_entry_schema::FIELD_VERSION: entry.denormalized_latest_file_info.version,
        data_room_entry_schema::FIELD_ACCESS_LEVEL: entry.denormalized_latest_file_info.access_level,
        data_room_entry_schema::FIELD_CHANGE_BY: entry.denormalized_latest_file_info.change_by,
        data_room_entry_schema::FIELD_DESCRIPTION: entry.denormalized_latest_file_info.description,
        data_room_entry_schema::FIELD_CATEGORIES: entry.denormalized_latest_file_info.categories,
        data_room_entry_schema::FIELD_TAGS: entry.denormalized_latest_file_info.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Announcements
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_announcement_from_global_entity(
    global_announcement: &MoleculeGlobalAnnouncement,
) -> serde_json::Value {
    serde_json::json!({
        announcement_schema::FIELD_CREATED_AT: global_announcement.announcement.system_time,
        announcement_schema::FIELD_UPDATED_AT: global_announcement.announcement.system_time,
        announcement_schema::FIELD_IPNFT_UID: global_announcement.ipnft_uid,
        announcement_schema::FIELD_HEADLINE: global_announcement.announcement.headline,
        announcement_schema::FIELD_BODY: global_announcement.announcement.body,
        announcement_schema::FIELD_ATTACHMENTS: global_announcement.announcement.attachments,
        announcement_schema::FIELD_ACCESS_LEVEL: global_announcement.announcement.access_level,
        announcement_schema::FIELD_CHANGE_BY: global_announcement.announcement.change_by,
        announcement_schema::FIELD_CATEGORIES: global_announcement.announcement.categories,
        announcement_schema::FIELD_TAGS: global_announcement.announcement.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_announcement_from_publication_record(
    ipnft_uid: &str,
    announcement_record: &MoleculeAnnouncementPayloadRecord,
    system_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        announcement_schema::FIELD_CREATED_AT: system_time,
        announcement_schema::FIELD_UPDATED_AT: system_time,
        announcement_schema::FIELD_IPNFT_UID: ipnft_uid,
        announcement_schema::FIELD_HEADLINE: announcement_record.headline,
        announcement_schema::FIELD_BODY: announcement_record.body,
        announcement_schema::FIELD_ATTACHMENTS: announcement_record.attachments,
        announcement_schema::FIELD_ACCESS_LEVEL: announcement_record.access_level,
        announcement_schema::FIELD_CHANGE_BY: announcement_record.change_by,
        announcement_schema::FIELD_CATEGORIES: announcement_record.categories,
        announcement_schema::FIELD_TAGS: announcement_record.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Activity records
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_data_room_activity(
    activity: &kamu_molecule_domain::MoleculeDataRoomActivity,
) -> serde_json::Value {
    serde_json::json!({
        activity_schema::FIELD_CREATED_AT: activity.event_time,
        activity_schema::FIELD_UPDATED_AT: activity.event_time,
        activity_schema::FIELD_IPNFT_UID: activity.ipnft_uid,
        activity_schema::FIELD_ENTRY_PATH: activity.path.to_string(),
        activity_schema::FIELD_ENTRY_REF: activity.r#ref.to_string(),
        activity_schema::FIELD_ACCESS_LEVEL: activity.access_level,
        activity_schema::FIELD_CHANGE_BY: activity.change_by,
        activity_schema::FIELD_DESCRIPTION: activity.description,
        activity_schema::FIELD_TAGS: activity.tags,
        activity_schema::FIELD_CATEGORIES: activity.categories,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_activity_from_data_room_publication_record(
    activity_record: &MoleculeDataRoomActivityPayloadRecord,
    event_time: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        activity_schema::FIELD_CREATED_AT: event_time,
        activity_schema::FIELD_UPDATED_AT: event_time,
        activity_schema::FIELD_IPNFT_UID: activity_record.ipnft_uid,
        activity_schema::FIELD_ENTRY_PATH: activity_record.path.to_string(),
        activity_schema::FIELD_ENTRY_REF: activity_record.r#ref.to_string(),
        activity_schema::FIELD_ACCESS_LEVEL: activity_record.access_level,
        activity_schema::FIELD_CHANGE_BY: activity_record.change_by,
        activity_schema::FIELD_DESCRIPTION: activity_record.description,
        activity_schema::FIELD_TAGS: activity_record.tags,
        activity_schema::FIELD_CATEGORIES: activity_record.categories,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
