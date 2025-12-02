// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::auth;
use odf::serde::DatasetDefaultVocabularySystemColumns;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeCreateAnnouncementUseCase)]
pub struct MoleculeCreateAnnouncementUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
    push_ingest_use_case: Arc<dyn kamu_core::PushIngestDataUseCase>,
}

impl MoleculeCreateAnnouncementUseCaseImpl {
    async fn validate_attachments(
        &self,
        announcement_data_record: &MoleculeGlobalAnnouncementDataRecord,
    ) -> Result<(), MoleculeCreateAnnouncementError> {
        if announcement_data_record.attachments.is_empty() {
            // Nothing to validate
            return Ok(());
        }

        let dataset_refs = announcement_data_record
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
        project_announcements_file_dataset: kamu_datasets::WriteCheckedDataset<'_>,
        mut global_announcement: MoleculeGlobalAnnouncementDataRecord,
    ) -> Result<MoleculeCreateAnnouncementResult, MoleculeCreateAnnouncementError> {
        // TODO: Align timestamps with ingest
        let now = Utc::now();

        // 1. Resolve global announcements dataset

        let global_announcements_dataset = self
            .molecule_dataset_service
            .get_global_announcements_dataset(
                &molecule_subject.account_name,
                auth::DatasetAction::Write,
                // TODO: try to create once as start-up job?
                true,
            )
            .await
            .map_err(|e| -> MoleculeCreateAnnouncementError {
                use MoleculeGetDatasetError as E;

                match e {
                    MoleculeGetDatasetError::NotFound(_) => {
                        unreachable!()
                    }
                    MoleculeGetDatasetError::Access(e) => e.into(),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        // 2. Validate input data

        self.validate_attachments(&global_announcement).await?;

        // 3. Store global announcements

        let new_announcement_id = uuid::Uuid::new_v4();

        global_announcement.announcement_id = Some(new_announcement_id);

        let global_announcement_record = MoleculeGlobalAnnouncementRecord {
            system_columns: DatasetDefaultVocabularySystemColumns {
                offset: None,
                op: odf::metadata::OperationType::Append,
                system_time: now,
                event_time: now,
            },
            record: global_announcement,
        };
        let project_announcement_record =
            global_announcement_record.as_project_announcement_record();

        self.push_ingest_use_case
            .execute(
                global_announcements_dataset,
                kamu_core::DataSource::Buffer(global_announcement_record.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(file_utils::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        // 4. Store project announcements

        self.push_ingest_use_case
            .execute(
                project_announcements_file_dataset.clone(),
                kamu_core::DataSource::Buffer(project_announcement_record.to_bytes()),
                kamu_core::PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(file_utils::MediaType::NDJSON.to_owned()),
                    expected_head: None,
                },
                None,
            )
            .await
            .int_err()?;

        Ok(MoleculeCreateAnnouncementResult {
            new_announcement_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
