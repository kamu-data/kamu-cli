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
    molecule_activity_search_schema as activity_schema,
    molecule_search_schema_common as molecule_schema,
};
use kamu_search::{SearchIndexUpdateOperation, SearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_data_room_activity(
    molecule_account_id: &odf::AccountID,
    activity: &MoleculeDataRoomActivity,
) -> serde_json::Value {
    serde_json::json!({
        molecule_schema::fields::EVENT_TIME: activity.event_time,
        molecule_schema::fields::SYSTEM_TIME: activity.system_time,
        activity_schema::fields::OFFSET: activity.offset,
        molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id,
        molecule_schema::fields::IPNFT_UID: activity.ipnft_uid,
        activity_schema::fields::ACTIVITY_TYPE: activity.activity_type,
        molecule_schema::fields::PATH: activity.path.to_string(),
        molecule_schema::fields::REF: activity.r#ref.to_string(),
        molecule_schema::fields::VERSION: activity.version,
        molecule_schema::fields::CONTENT_TYPE: activity.content_type,
        molecule_schema::fields::CONTENT_HASH: activity.content_hash,
        molecule_schema::fields::CONTENT_LENGTH: activity.content_length,
        molecule_schema::fields::ACCESS_LEVEL: activity.access_level,
        molecule_schema::fields::CHANGE_BY: activity.change_by,
        molecule_schema::fields::DESCRIPTION: activity.description,
        molecule_schema::fields::TAGS: activity.tags,
        molecule_schema::fields::CATEGORIES: activity.categories,
        kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
        kamu_search::fields::PRINCIPAL_IDS: vec![ molecule_account_id.to_string() ],
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_activity_from_data_room_publication_record(
    molecule_account_id: &odf::AccountID,
    activity_record: &MoleculeDataRoomActivityPayloadRecord,
    system_time: DateTime<Utc>,
    event_time: DateTime<Utc>,
    offset: u64,
) -> serde_json::Value {
    serde_json::json!({
        molecule_schema::fields::EVENT_TIME: event_time,
        molecule_schema::fields::SYSTEM_TIME: system_time,
        activity_schema::fields::OFFSET: offset,
        molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id,
        molecule_schema::fields::IPNFT_UID: activity_record.ipnft_uid,
        activity_schema::fields::ACTIVITY_TYPE: activity_record.activity_type,
        molecule_schema::fields::PATH: activity_record.path.to_string(),
        molecule_schema::fields::REF: activity_record.r#ref.to_string(),
        molecule_schema::fields::VERSION: activity_record.version,
        molecule_schema::fields::CONTENT_TYPE: activity_record.content_type,
        molecule_schema::fields::CONTENT_HASH: activity_record.content_hash,
        molecule_schema::fields::CONTENT_LENGTH: activity_record.content_length,
        molecule_schema::fields::ACCESS_LEVEL: activity_record.access_level,
        molecule_schema::fields::CHANGE_BY: activity_record.change_by,
        molecule_schema::fields::DESCRIPTION: activity_record.description,
        molecule_schema::fields::TAGS: activity_record.tags,
        molecule_schema::fields::CATEGORIES: activity_record.categories,
        kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
        kamu_search::fields::PRINCIPAL_IDS: vec![ molecule_account_id.to_string() ],
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_activities(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn SearchRepository,
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

            operations.push(SearchIndexUpdateOperation::Index { id, doc: document });
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
