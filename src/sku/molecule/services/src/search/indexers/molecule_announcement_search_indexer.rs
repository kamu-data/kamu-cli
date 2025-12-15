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
    MoleculeAnnouncementPayloadRecord,
    MoleculeGlobalAnnouncement,
    MoleculeViewGlobalAnnouncementsUseCase,
    molecule_announcement_full_text_search_schema as announcement_schema,
};
use kamu_search::{FullTextSearchRepository, FullTextUpdateOperation};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 500;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
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
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_announcements(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn FullTextSearchRepository,
) -> Result<usize, InternalError> {
    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        "Indexing global announcements for Molecule organization account",
    );

    let mut total_documents_count = 0;
    let mut operations = Vec::new();
    let mut offset = 0;

    let molecule_view_global_announcements_uc = catalog
        .get_one::<dyn MoleculeViewGlobalAnnouncementsUseCase>()
        .unwrap();

    loop {
        // Load announcements in pages aligned with bulk size
        let announcements_listing = molecule_view_global_announcements_uc
            .execute(
                organization_account,
                None, /* no filters */
                Some(PaginationOpts {
                    limit: BULK_SIZE,
                    offset,
                }),
            )
            .await
            .int_err()?;

        // Break if no more announcements
        if announcements_listing.list.is_empty() {
            break;
        }

        // Index each announcement
        for announcement in announcements_listing.list {
            // Serialize announcement into search document
            let document = index_announcement_from_global_entity(&announcement);

            operations.push(FullTextUpdateOperation::Index {
                id: announcement.announcement.announcement_id.to_string(),
                doc: document,
            });
        }

        // Bulk index the current page
        if !operations.is_empty() {
            let batch_count = operations.len();
            tracing::debug!(
                documents_count = batch_count,
                "Bulk indexing announcements batch",
            );
            repo.bulk_update(announcement_schema::SCHEMA_NAME, operations)
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
        "Indexed global announcements for Molecule organization account",
    );

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
