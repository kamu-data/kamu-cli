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
#[dill::interface(dyn MoleculeDisableProjectUseCase)]
pub struct MoleculeDisableProjectUseCaseImpl {
    project_service: Arc<dyn MoleculeProjectService>,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeDisableProjectUseCase for MoleculeDisableProjectUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeDisableProjectUseCaseImpl_execute,
        skip_all,
        fields(?ipnft_uid)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeDisableProjectError> {
        use datafusion::prelude::*;

        let now = chrono::Utc::now();

        let (projects_dataset, df_opt) = self
            .project_service
            .get_projects_changelog_data_frame(molecule_subject, DatasetAction::Write, false)
            .await?;

        let Some(df): Option<odf::utils::data::DataFrameExt> = df_opt else {
            return self
                .existing_project_from_ledger(molecule_subject, ipnft_uid)
                .await;
        };

        let df = df.filter(col("ipnft_uid").eq(lit(&ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        let Some(record) = records.into_iter().next() else {
            return self
                .existing_project_from_ledger(molecule_subject, ipnft_uid)
                .await;
        };

        let mut project = MoleculeProjectEntity::from_json(record).int_err()?;
        project.system_time = now;
        project.event_time = now;

        let changelog_record =
            project.into_changelog_record(u8::from(odf::metadata::OperationType::Retract));

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
                MoleculeProjectMessage::disabled(
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

impl MoleculeDisableProjectUseCaseImpl {
    async fn existing_project_from_ledger(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeDisableProjectError> {
        use datafusion::prelude::*;

        let (_, ledger_opt) = self
            .project_service
            .get_projects_raw_ledger_data_frame(molecule_subject, DatasetAction::Write, false)
            .await?;

        let Some(ledger_df) = ledger_opt else {
            return Err(MoleculeDisableProjectError::ProjectNotFound(
                ProjectNotFoundError { ipnft_uid },
            ));
        };

        let df = ledger_df
            .filter(col("ipnft_uid").eq(lit(&ipnft_uid)))
            .int_err()?
            .sort(vec![col("system_time").sort(false, false)])
            .int_err()?;

        let records: Vec<serde_json::Value> = df.collect_json_aos().await.int_err()?;
        let record = records.into_iter().find(|record| {
            record
                .get("op")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u8::try_from(value).ok())
                .and_then(|value| OperationType::try_from(value).ok())
                .map(|op| matches!(op, OperationType::Append | OperationType::CorrectTo))
                .unwrap_or(false)
        });

        let Some(record) = record else {
            return Err(MoleculeDisableProjectError::ProjectNotFound(
                ProjectNotFoundError { ipnft_uid },
            ));
        };

        Ok(MoleculeProjectEntity::from_json(record).int_err()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
