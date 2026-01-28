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
    MoleculeViewGlobalAnnouncementsMode,
    MoleculeViewGlobalAnnouncementsUseCase,
    molecule_announcement_search_schema as announcement_schema,
    molecule_search_schema_common as molecule_schema,
};
use kamu_search::{
    EmbeddingsChunker,
    EmbeddingsEncoder,
    SearchFieldUpdate,
    SearchIndexUpdateOperation,
    SearchRepository,
    prepare_semantic_embeddings_document,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing helper
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct MoleculeAnnouncementIndexingHelper<'a> {
    pub embeddings_chunker: &'a dyn EmbeddingsChunker,
    pub embeddings_encoder: &'a dyn EmbeddingsEncoder,
}

impl MoleculeAnnouncementIndexingHelper<'_> {
    pub(crate) async fn index_announcement_from_global_entity(
        &self,
        molecule_account_id: &odf::AccountID,
        global_announcement: &MoleculeGlobalAnnouncement,
    ) -> Result<serde_json::Value, InternalError> {
        let mut index_doc = serde_json::json!({
            molecule_schema::fields::EVENT_TIME: global_announcement.announcement.event_time,
            molecule_schema::fields::SYSTEM_TIME: global_announcement.announcement.system_time,
            molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id.to_string(),
            molecule_schema::fields::IPNFT_UID: global_announcement.ipnft_uid,
            announcement_schema::fields::HEADLINE: global_announcement.announcement.headline,
            announcement_schema::fields::BODY: global_announcement.announcement.body,
            announcement_schema::fields::ATTACHMENTS: global_announcement.announcement.attachments,
            molecule_schema::fields::ACCESS_LEVEL: global_announcement.announcement.access_level,
            molecule_schema::fields::CHANGE_BY: global_announcement.announcement.change_by,
            molecule_schema::fields::CATEGORIES: global_announcement.announcement.categories,
            molecule_schema::fields::TAGS: global_announcement.announcement.tags,
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![ molecule_account_id.to_string() ],
        });

        self.attach_embeddings(
            &mut index_doc,
            &global_announcement.announcement.headline,
            &global_announcement.announcement.body,
        )
        .await?;

        Ok(index_doc)
    }

    pub(crate) async fn index_announcement_from_publication_record(
        &self,
        event_time: DateTime<Utc>,
        system_time: DateTime<Utc>,
        molecule_account_id: &odf::AccountID,
        ipnft_uid: &str,
        announcement_record: &MoleculeAnnouncementPayloadRecord,
    ) -> Result<serde_json::Value, InternalError> {
        let mut index_doc = serde_json::json!({
            molecule_schema::fields::EVENT_TIME: event_time,
            molecule_schema::fields::SYSTEM_TIME: system_time,
            molecule_schema::fields::MOLECULE_ACCOUNT_ID: molecule_account_id.to_string(),
            molecule_schema::fields::IPNFT_UID: ipnft_uid,
            announcement_schema::fields::HEADLINE: announcement_record.headline,
            announcement_schema::fields::BODY: announcement_record.body,
            announcement_schema::fields::ATTACHMENTS: announcement_record.attachments,
            molecule_schema::fields::ACCESS_LEVEL: announcement_record.access_level,
            molecule_schema::fields::CHANGE_BY: announcement_record.change_by,
            molecule_schema::fields::CATEGORIES: announcement_record.categories,
            molecule_schema::fields::TAGS: announcement_record.tags,
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![ molecule_account_id.to_string() ],
        });

        self.attach_embeddings(
            &mut index_doc,
            &announcement_record.headline,
            &announcement_record.body,
        )
        .await?;

        Ok(index_doc)
    }

    async fn attach_embeddings(
        &self,
        index_doc: &mut serde_json::Value,
        headline: &str,
        body: &str,
    ) -> Result<(), InternalError> {
        let index_doc_mut = index_doc.as_object_mut().unwrap();

        let headline_update = SearchFieldUpdate::Present(headline);
        let body_update = SearchFieldUpdate::Present(body);

        let embeddings_document = prepare_semantic_embeddings_document(
            self.embeddings_chunker,
            self.embeddings_encoder,
            &[&headline_update, &body_update],
        )
        .await?;

        if let SearchFieldUpdate::Present(v) = embeddings_document {
            index_doc_mut.insert(
                kamu_search::fields::SEMANTIC_EMBEDDINGS.to_string(),
                serde_json::json!(v),
            );
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_announcements(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn SearchRepository,
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

    let embeddings_chunker = catalog.get_one::<dyn EmbeddingsChunker>().unwrap();
    let embeddings_encoder = catalog.get_one::<dyn EmbeddingsEncoder>().unwrap();

    let indexing_helper = MoleculeAnnouncementIndexingHelper {
        embeddings_chunker: embeddings_chunker.as_ref(),
        embeddings_encoder: embeddings_encoder.as_ref(),
    };

    loop {
        // Load announcements in pages aligned with bulk size
        let announcements_listing = molecule_view_global_announcements_uc
            .execute(
                organization_account,
                MoleculeViewGlobalAnnouncementsMode::LatestSource,
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
            let document = indexing_helper
                .index_announcement_from_global_entity(
                    &organization_account.account_id,
                    &announcement,
                )
                .await?;

            operations.push(SearchIndexUpdateOperation::Index {
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
