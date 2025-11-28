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

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeRemoveProjectUseCase)]
pub struct MoleculeRemoveProjectUseCaseImpl {
    project_service: Arc<dyn MoleculeProjectService>,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeRemoveProjectUseCase for MoleculeRemoveProjectUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeRemoveProjectUseCaseImpl_execute,
        skip_all,
        fields(?ipnft_uid)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeRemoveProjectError> {
        // Access the projects dataset snapshot
        let (projects_dataset, df) = self
            .project_service
            .get_projects_data_frame(molecule_subject, DatasetAction::Write, false)
            .await?;

        let Some(df) = df else {
            return Err(MoleculeRemoveProjectError::ProjectNotFound { ipnft_uid });
        };

        use datafusion::prelude::*;

        let df = df.filter(col("ipnft_uid").eq(lit(&ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        let Some(record) = records.into_iter().next() else {
            return Err(MoleculeRemoveProjectError::ProjectNotFound { ipnft_uid });
        };

        let mut project = MoleculeProjectEntity::from_json(record).int_err()?;
        let now = chrono::Utc::now();
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

        Ok(project)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
