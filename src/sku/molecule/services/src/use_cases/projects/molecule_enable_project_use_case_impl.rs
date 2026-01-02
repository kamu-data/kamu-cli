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
use kamu_accounts::LoggedAccount;
use kamu_core::PushIngestResult;
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeProjectsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeEnableProjectUseCase)]
pub struct MoleculeEnableProjectUseCaseImpl {
    projects_service: Arc<dyn MoleculeProjectsService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeEnableProjectUseCase for MoleculeEnableProjectUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeEnableProjectUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        source_event_time: Option<DateTime<Utc>>,
        ipnft_uid: String,
    ) -> Result<MoleculeProject, MoleculeEnableProjectError> {
        use datafusion::prelude::*;

        // Gain write access to projects dataset
        let projects_writer = self
            .projects_service
            .writer(&molecule_subject.account_name, false)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeEnableProjectError>)?;

        // Observe raw ledger DF to find the project
        let maybe_raw_ledger_df = projects_writer
            .as_reader()
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeEnableProjectError>)?;

        // No records at all yet?
        let Some(ledger_df): Option<odf::utils::data::DataFrameExt> = maybe_raw_ledger_df else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        // Find the latest ledger record for the given ipnft_uid
        let df = ledger_df
            .filter(col("ipnft_uid").eq(lit(&ipnft_uid)))
            .int_err()?
            .sort(vec![col("offset").sort(false, false)])
            .int_err()?
            .limit(0, Some(1))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        assert!(records.len() <= 1);

        // No record found?
        let Some(record) = records.into_iter().next() else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        // Reconstruct last changelog entry available
        let last_changelog_entry = MoleculeProjectChangelogEntry::from_json(record).int_err()?;

        // Idempotent handling: if already enabled, return as is
        if last_changelog_entry.system_columns.op != odf::metadata::OperationType::Retract {
            // Project is already enabled
            return Ok(MoleculeProject::from_payload(
                last_changelog_entry.payload,
                last_changelog_entry
                    .system_columns
                    .timestamp_columns
                    .system_time,
                last_changelog_entry
                    .system_columns
                    .timestamp_columns
                    .event_time,
            )?);
        }

        // Add Append record to re-enable the project
        let new_changelog_record = MoleculeProjectChangelogInsertionRecord {
            op: odf::metadata::OperationType::Append,
            payload: last_changelog_entry.payload,
        };

        let push_res = projects_writer
            .push_ndjson_data(new_changelog_record.to_bytes(), source_event_time)
            .await
            .int_err()?;

        match push_res {
            PushIngestResult::UpToDate => unreachable!(),
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                ..
            } => {
                // Notify external listeners
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
                        MoleculeProjectMessage::reenabled(
                            source_event_time.unwrap_or(insertion_system_time),
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            new_changelog_record.payload.account_id.clone(),
                            new_changelog_record.payload.ipnft_uid.clone(),
                            new_changelog_record.payload.ipnft_symbol.clone(),
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
