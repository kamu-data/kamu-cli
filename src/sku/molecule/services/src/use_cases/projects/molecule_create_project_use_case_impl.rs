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
use kamu_accounts::{
    AccountService,
    AccountServiceExt,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    LoggedAccount,
};
use kamu_auth_rebac::RebacService;
use kamu_core::PushIngestResult;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetUseCaseOptions};
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeProjectsService;
use crate::services::MoleculeDatasetWriterPushNdjsonDataOptions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeCreateProjectUseCase)]
pub struct MoleculeCreateProjectUseCaseImpl {
    projects_service: Arc<dyn MoleculeProjectsService>,
    account_service: Arc<dyn AccountService>,
    create_account_use_case: Arc<dyn CreateAccountUseCase>,
    create_dataset_from_snapshot_use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    rebac_service: Arc<dyn RebacService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeCreateProjectUseCase for MoleculeCreateProjectUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeCreateProjectUseCaseImpl_execute,
        skip_all,
        fields(ocl_id, symbol)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        source_event_time: Option<DateTime<Utc>>,
        ocl_id: OclId,
        symbol: Symbol,
    ) -> Result<MoleculeProject, MoleculeCreateProjectError> {
        // Gain write access to projects dataset
        let projects_writer = self
            .projects_service
            .writer(&molecule_subject.account_name, true)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeCreateProjectError>)?;

        // Obtain raw ledger DF
        let maybe_raw_ledger_df = projects_writer
            .as_reader()
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeCreateProjectError>)?;

        use datafusion::prelude::*;

        // Check for conflicts
        if let Some(df) = maybe_raw_ledger_df {
            let df = df
                .filter(
                    col("ocl_id")
                        .eq(lit(ocl_id.as_ref()))
                        .or(lower(col("symbol")).eq(lit(symbol.as_ref()))),
                )
                .int_err()?
                .sort(vec![col("offset").sort(false, false)])
                .int_err()?
                .limit(0, Some(1))
                .int_err()?;

            // If any record found, it's a conflict
            let records = df.collect_json_aos().await.int_err()?;
            if let Some(record) = records.into_iter().next() {
                return Err(MoleculeCreateProjectError::Conflict {
                    project: MoleculeProject::from_json(record)?,
                });
            }
        }

        // Create a project account
        let molecule_account = self
            .account_service
            .try_get_account_by_id(&molecule_subject.account_id)
            .await?
            .unwrap();

        let project_account_name: odf::AccountName =
            format!("{}.{symbol}", molecule_account.account_name)
                .parse()
                .int_err()?;

        let project_email = format!("support+{project_account_name}@kamu.dev")
            .parse()
            .unwrap();

        // TODO: Molecule: Phase 3: looks we are ready
        // TODO: Remove tolerance to accounts that already exist after we have account
        // deletion api? Reusing existing accounts may be a security threat via
        // name squatting.
        let project_account = if let Some(acc) = self
            .account_service
            .account_by_name(&project_account_name)
            .await
            .int_err()?
        {
            acc
        } else {
            // TODO: Set avatar and display name?
            // https://avatars.githubusercontent.com/u/37688345?s=200&amp;v=4
            self.create_account_use_case
                .execute_derived(
                    &molecule_account,
                    &project_account_name,
                    CreateAccountUseCaseOptions::builder()
                        .maybe_email(Some(project_email))
                        .build(),
                )
                .await
                .int_err()?
        };

        // Create `data-room` dataset
        let snapshot = MoleculeDatasetSnapshots::data_room(project_account_name.clone());
        let data_room_create_res = self
            .create_dataset_from_snapshot_use_case
            .execute(
                snapshot,
                CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Create `announcements` dataset
        let snapshot = MoleculeDatasetSnapshots::announcements(project_account_name);
        let announcements_create_res = self
            .create_dataset_from_snapshot_use_case
            .execute(
                snapshot,
                CreateDatasetUseCaseOptions {
                    dataset_visibility: odf::DatasetVisibility::Private,
                },
            )
            .await
            .int_err()?;

        // Give maintainer permissions to molecule
        self.rebac_service
            .set_account_dataset_relation(
                &molecule_subject.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &data_room_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        self.rebac_service
            .set_account_dataset_relation(
                &molecule_subject.account_id,
                kamu_auth_rebac::AccountToDatasetRelation::Maintainer,
                &announcements_create_res.dataset_handle.id,
            )
            .await
            .int_err()?;

        // Add project entry
        let project_payload = MoleculeProjectPayloadRecord {
            ocl_id: ocl_id.clone(),
            symbol: symbol.clone(),
            odf_account_id: project_account.id.clone(),
            odf_data_room_dataset_id: data_room_create_res.dataset_handle.id,
            odf_announcements_dataset_id: announcements_create_res.dataset_handle.id,
        };

        let new_changelog_record = MoleculeProjectChangelogInsertionRecord {
            op: odf::metadata::OperationType::Append,
            payload: project_payload,
        };

        let push_res = projects_writer
            .push_ndjson_data(
                new_changelog_record.to_bytes(),
                MoleculeDatasetWriterPushNdjsonDataOptions {
                    source_event_time,
                    skip_quota_check: false,
                },
            )
            .await
            .int_err()?;

        match push_res {
            PushIngestResult::UpToDate => unreachable!(),
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                ..
            } => {
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
                        MoleculeProjectMessage::created(
                            source_event_time.unwrap_or(insertion_system_time),
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            project_account.id,
                            ocl_id,
                            symbol,
                        ),
                    )
                    .await
                    .int_err()?;

                Ok(MoleculeProject::from_payload(
                    new_changelog_record.payload,
                    insertion_system_time,
                    source_event_time.unwrap_or(insertion_system_time),
                )?)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
