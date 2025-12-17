// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::{
    MoleculeDataRoomActivity,
    MoleculeDataRoomActivityPayloadRecord,
    MoleculeGlobalActivity,
    MoleculeViewGlobalActivitiesMode,
    MoleculeViewGlobalActivitiesUseCase,
    molecule_activity_full_text_search_schema as activity_schema,
};
use kamu_search::{FullTextSearchRepository, FullTextUpdateOperation};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 500;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_data_room_activity(
    molecule_account_id: &odf::AccountID,
    activity: &MoleculeDataRoomActivity,
) -> serde_json::Value {
    serde_json::json!({
        activity_schema::FIELD_EVENT_TIME: activity.event_time,
        activity_schema::FIELD_SYSTEM_TIME: activity.system_time,
        activity_schema::FIELD_OFFSET: activity.offset,
        activity_schema::FIELD_MOLECULE_ACCOUNT_ID: molecule_account_id,
        activity_schema::FIELD_IPNFT_UID: activity.ipnft_uid,
        activity_schema::FIELD_ACTIVITY_TYPE: activity.activity_type,
        activity_schema::FIELD_PATH: activity.path.to_string(),
        activity_schema::FIELD_REF: activity.r#ref.to_string(),
        activity_schema::FIELD_VERSION: activity.version,
        activity_schema::FIELD_CONTENT_TYPE: activity.content_type,
        activity_schema::FIELD_CONTENT_HASH: activity.content_hash,
        activity_schema::FIELD_CONTENT_LENGTH: activity.content_length,
        activity_schema::FIELD_ACCESS_LEVEL: activity.access_level,
        activity_schema::FIELD_CHANGE_BY: activity.change_by,
        activity_schema::FIELD_DESCRIPTION: activity.description,
        activity_schema::FIELD_TAGS: activity.tags,
        activity_schema::FIELD_CATEGORIES: activity.categories,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_activity_from_data_room_publication_record(
    molecule_account_id: &odf::AccountID,
    activity_record: &MoleculeDataRoomActivityPayloadRecord,
    event_time: DateTime<Utc>,
    offset: u64,
) -> serde_json::Value {
    serde_json::json!({
        activity_schema::FIELD_EVENT_TIME: event_time,
        activity_schema::FIELD_SYSTEM_TIME: event_time,
        activity_schema::FIELD_OFFSET: offset,
        activity_schema::FIELD_MOLECULE_ACCOUNT_ID: molecule_account_id,
        activity_schema::FIELD_IPNFT_UID: activity_record.ipnft_uid,
        activity_schema::FIELD_ACTIVITY_TYPE: activity_record.activity_type,
        activity_schema::FIELD_PATH: activity_record.path.to_string(),
        activity_schema::FIELD_REF: activity_record.r#ref.to_string(),
        activity_schema::FIELD_VERSION: activity_record.version,
        activity_schema::FIELD_CONTENT_TYPE: activity_record.content_type,
        activity_schema::FIELD_CONTENT_HASH: activity_record.content_hash,
        activity_schema::FIELD_CONTENT_LENGTH: activity_record.content_length,
        activity_schema::FIELD_ACCESS_LEVEL: activity_record.access_level,
        activity_schema::FIELD_CHANGE_BY: activity_record.change_by,
        activity_schema::FIELD_DESCRIPTION: activity_record.description,
        activity_schema::FIELD_TAGS: activity_record.tags,
        activity_schema::FIELD_CATEGORIES: activity_record.categories,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_activities(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn FullTextSearchRepository,
) -> Result<usize, InternalError> {
    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        "Indexing global activities for Molecule organization account",
    );

    let mut total_documents_count = 0;
    let mut operations = Vec::new();
    let mut offset = 0;

    let molecule_view_global_activities_uc = catalog
        .get_one::<dyn MoleculeViewGlobalActivitiesUseCase>()
        .unwrap();

    loop {
        // Load activities in pages aligned with bulk size
        let activities_listing = molecule_view_global_activities_uc
            .execute(
                organization_account,
                MoleculeViewGlobalActivitiesMode::LatestSource,
                None, /* no filters */
                Some(PaginationOpts {
                    limit: BULK_SIZE,
                    offset,
                }),
            )
            .await
            .int_err()?;

        // Break if no more activities
        if activities_listing.list.is_empty() {
            break;
        }

        // Index each activity
        for activity in activities_listing.list {
            // Skip announcement activities
            let data_room_activity = match activity {
                MoleculeGlobalActivity::DataRoomActivity(dr_activity) => dr_activity,
                MoleculeGlobalActivity::Announcement(_) => continue,
            };

            // Serialize activity into search document
            let document =
                index_data_room_activity(&organization_account.account_id, &data_room_activity);

            // Generate unique ID using molecule_account_id and the activity's offset
            let id = activity_schema::unique_id_for_data_room_activity(
                &organization_account.account_id,
                data_room_activity.offset,
            );

            operations.push(FullTextUpdateOperation::Index { id, doc: document });
        }

        // Bulk index the current page
        if !operations.is_empty() {
            let batch_count = operations.len();
            tracing::debug!(
                documents_count = batch_count,
                "Bulk indexing activities batch",
            );
            repo.bulk_update(activity_schema::SCHEMA_NAME, operations)
                .await?;
            total_documents_count += batch_count;
            operations = Vec::new();
        }

        // Move to next page
        offset += BULK_SIZE;
    }

    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        indexed_documents_count = total_documents_count,
        "Indexed global activities for Molecule organization account",
    );

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
