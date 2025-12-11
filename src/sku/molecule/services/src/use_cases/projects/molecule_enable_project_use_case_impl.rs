// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};
use odf::metadata::OperationType;

use crate::MoleculeProjectsDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeEnableProjectUseCase)]
pub struct MoleculeEnableProjectUseCaseImpl {
    molecule_projects_dataset_service: Arc<dyn MoleculeProjectsDatasetService>,
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
        ipnft_uid: String,
    ) -> Result<MoleculeProject, MoleculeEnableProjectError> {
        use datafusion::prelude::*;

        // Gain write access to projects dataset
        let projects_writer = self
            .molecule_projects_dataset_service
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

        let records: Vec<serde_json::Value> = df.collect_json_aos().await.int_err()?;
        assert!(records.len() <= 1);

        // No record found?
        let Some(record) = records.into_iter().next() else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        // Decode operation type
        let op = record
            .get("op")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .and_then(|value| OperationType::try_from(value).ok());

        // TODO: maybe internal error instead?
        let Some(op) = op else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        // Reconstruct project entity
        let mut project = MoleculeProject::from_json(record).int_err()?;
        project.ipnft_symbol.make_ascii_lowercase();

        // Idempotent handling: ignore if not retracted
        if !matches!(op, OperationType::Retract) {
            return Ok(project);
        }

        // Add Append record to re-enable the project
        let now = chrono::Utc::now();
        project.system_time = now;
        project.event_time = now;

        let changelog_record = project.as_changelog_record(u8::from(OperationType::Append));

        projects_writer
            .push_ndjson_data(changelog_record.to_bytes(), Some(now))
            .await
            .int_err()?;

        // Notify external listeners
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
                MoleculeProjectMessage::reenabled(
                    now,
                    molecule_subject.account_id.clone(),
                    project.account_id.clone(),
                    project.ipnft_uid.clone(),
                    project.ipnft_symbol.clone(),
                ),
            )
            .await
            .int_err()?;

        Ok(project)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
