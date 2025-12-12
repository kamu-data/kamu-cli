// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::ResultIntoInternal;
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::{PushIngestResult, auth};
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeAnnouncementsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeCreateAnnouncementUseCase)]
pub struct MoleculeCreateAnnouncementUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
    outbox: Arc<dyn Outbox>,
}

impl MoleculeCreateAnnouncementUseCaseImpl {
    async fn validate_attachments(
        &self,
        announcement: &MoleculeAnnouncementPayloadRecord,
    ) -> Result<(), MoleculeCreateAnnouncementError> {
        if announcement.attachments.is_empty() {
            // Nothing to validate
            return Ok(());
        }

        let dataset_refs = announcement
            .attachments
            .iter()
            .map(odf::DatasetID::as_local_ref)
            .collect::<Vec<_>>();
        let dataset_refs_as_refs = dataset_refs.iter().collect::<Vec<_>>();

        let resolution = self
            .rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(&dataset_refs_as_refs, auth::DatasetAction::Read)
            .await?;

        if !resolution.inaccessible_refs.is_empty() {
            let not_found_dataset_ids = resolution
                .inaccessible_refs
                .into_iter()
                // Safety: having ID as input guarantees ID as output
                .map(|(dataset_ref, _e)| dataset_ref.into_id().unwrap())
                .collect();

            return Err(MoleculeCreateAnnouncementNotFoundAttachmentsError {
                not_found_dataset_ids,
            }
            .into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeCreateAnnouncementUseCase for MoleculeCreateAnnouncementUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeCreateAnnouncementUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        announcement: MoleculeAnnouncementPayloadRecord,
    ) -> Result<MoleculeCreateAnnouncementResult, MoleculeCreateAnnouncementError> {
        let new_announcement_id = announcement.announcement_id;

        // 1. Validate input data

        self.validate_attachments(&announcement).await?;

        // 2. Store global announcement

        let global_announcement_record = MoleculeGlobalAnnouncementChangelogInsertionRecord {
            op: odf::metadata::OperationType::Append,
            payload: MoleculeGlobalAnnouncementPayloadRecord {
                ipnft_uid: molecule_project.ipnft_uid.clone(),
                announcement,
            },
        };

        let global_announcements_writer = self
            .announcements_service
            .global_writer(&molecule_subject.account_name, true) // TODO: try to create once as start-up job?
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeCreateAnnouncementError>)?;

        let push_res = global_announcements_writer
            .push_ndjson_data(global_announcement_record.to_bytes(), source_event_time)
            .await?;

        assert!(matches!(push_res, PushIngestResult::Updated { .. }));

        // 3. Store project announcement

        let project_announcement_record = global_announcement_record.into_announcement_record();

        let project_announcements_writer = self
            .announcements_service
            .project_writer(molecule_project)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeCreateAnnouncementError>)?;

        let push_res = project_announcements_writer
            .push_ndjson_data(project_announcement_record.to_bytes(), source_event_time)
            .await?;

        match push_res {
            PushIngestResult::UpToDate => {
                unreachable!("We just created a new announcement, it cannot be up-to-date")
            }
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                ..
            } => {
                // 4. Notify external listeners
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_ANNOUNCEMENT_SERVICE,
                        MoleculeAnnouncementMessage::created(
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            molecule_subject.account_id.clone(),
                            molecule_project.ipnft_uid.clone(),
                            project_announcement_record.payload,
                        ),
                    )
                    .await
                    .int_err()?;

                Ok(MoleculeCreateAnnouncementResult {
                    new_announcement_id,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
