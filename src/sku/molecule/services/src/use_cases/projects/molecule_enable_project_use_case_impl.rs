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
use kamu_core::PushIngestDataUseCase;
use kamu_core::auth::DatasetAction;
use messaging_outbox::{Outbox, OutboxExt};
use odf::metadata::OperationType;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeEnableProjectUseCase)]
pub struct MoleculeEnableProjectUseCaseImpl {
    molecule_projects_dataset_service: Arc<dyn MoleculeProjectsDatasetService>,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
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

        let (projects_dataset, df_opt) = self
            .molecule_projects_dataset_service
            .get_projects_raw_ledger_data_frame(molecule_subject, DatasetAction::Write, false)
            .await?;

        let Some(ledger_df): Option<odf::utils::data::DataFrameExt> = df_opt else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        let df = ledger_df
            .filter(col("ipnft_uid").eq(lit(&ipnft_uid)))
            .int_err()?
            .sort(vec![col("offset").sort(false, false)])
            .int_err()?
            .limit(0, Some(1))
            .int_err()?;

        let records: Vec<serde_json::Value> = df.collect_json_aos().await.int_err()?;
        let Some(record) = records.into_iter().next() else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        let op = record
            .get("op")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .and_then(|value| OperationType::try_from(value).ok());

        let Some(op) = op else {
            return Err(MoleculeEnableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        let mut project = MoleculeProject::from_json(record).int_err()?;
        project.ipnft_symbol.make_ascii_lowercase();

        if !matches!(op, OperationType::Retract) {
            return Ok(project);
        }

        let now = chrono::Utc::now();
        project.system_time = now;
        project.event_time = now;

        let changelog_record = project.as_changelog_record(u8::from(OperationType::Append));

        self.push_ingest_use_case
            .execute(
                projects_dataset,
                kamu_core::DataSource::Buffer(changelog_record.to_bytes()),
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
