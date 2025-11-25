// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;
use kamu_core::DatasetRegistryExt;
use kamu_molecule_domain::{
    CreateMoleculeProjectError,
    CreateMoleculeProjectUseCase,
    FindMoleculeProjectError,
    FindMoleculeProjectUseCase,
};

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::molecule::v1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct MoleculeMutV1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV1 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV1_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    pub async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: U256,
    ) -> Result<CreateProjectResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let create_molecule_project = from_catalog_n!(ctx, dyn CreateMoleculeProjectUseCase);
        let project_json = match create_molecule_project
            .execute(
                &molecule_subject,
                ipnft_symbol,
                ipnft_uid,
                ipnft_address,
                ipnft_token_id.as_ref().clone(),
            )
            .await
        {
            Ok(project_json) => project_json,
            Err(CreateMoleculeProjectError::Conflict { project }) => {
                let project = v1::MoleculeProject::from_json(project);
                return Ok(CreateProjectResult::Conflict(CreateProjectErrorConflict {
                    project,
                }));
            }
            Err(CreateMoleculeProjectError::Access(e)) => return Err(GqlError::Access(e)),
            Err(CreateMoleculeProjectError::Internal(e)) => {
                return Err(GqlError::Gql(e.into()));
            }
        };

        let project = v1::MoleculeProject::from_json(project_json);
        Ok(CreateProjectResult::Success(CreateProjectSuccess {
            project,
        }))
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV1_project, skip_all, fields(?ipnft_uid))]
    pub async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMut>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_molecule_project = from_catalog_n!(ctx, dyn FindMoleculeProjectUseCase);
        let maybe_project_json = find_molecule_project
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                FindMoleculeProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                FindMoleculeProjectError::Access(e) => GqlError::Access(e),
                FindMoleculeProjectError::Internal(e) => GqlError::Gql(e.into()),
            })?;

        let maybe_project = maybe_project_json.map(MoleculeProjectMut::from_json);
        Ok(maybe_project)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(dead_code)]
pub struct MoleculeProjectMut {
    pub account_id: odf::AccountID,
    pub data_room_dataset_id: odf::DatasetID,
    pub announcements_dataset_id: odf::DatasetID,
}

impl MoleculeProjectMut {
    pub fn from_json(record: serde_json::Value) -> Self {
        let serde_json::Value::Object(record) = record else {
            unreachable!()
        };

        let account_id =
            odf::AccountID::from_did_str(record["account_id"].as_str().unwrap()).unwrap();

        let data_room_dataset_id =
            odf::DatasetID::from_did_str(record["data_room_dataset_id"].as_str().unwrap()).unwrap();

        let announcements_dataset_id =
            odf::DatasetID::from_did_str(record["announcements_dataset_id"].as_str().unwrap())
                .unwrap();

        Self {
            account_id,
            data_room_dataset_id,
            announcements_dataset_id,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMut {
    /// Creates an announcement record for the project
    #[tracing::instrument(level = "info", name = MoleculeProjectMut_create_announcement, skip_all)]
    async fn create_announcement(
        &self,
        ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<String>>,
        molecule_access_level: String,
        molecule_change_by: String,
    ) -> Result<CreateAnnouncementResult> {
        let (dataset_reg, push_ingest_use_case) = from_catalog_n!(
            ctx,
            dyn domain::DatasetRegistry,
            dyn domain::PushIngestDataUseCase
        );

        // Validate attachment links
        let attachments = attachments.unwrap_or_default();
        for att in &attachments {
            let did = match odf::DatasetID::from_did_str(att) {
                Ok(did) => did,
                Err(err) => {
                    return Ok(CreateAnnouncementResult::InvalidAttachment(
                        CreateAnnouncementErrorInvalidAttachment {
                            message: err.to_string(),
                        },
                    ));
                }
            };

            if dataset_reg
                .try_resolve_dataset_handle_by_ref(&did.as_local_ref())
                .await?
                .is_none()
            {
                return Ok(CreateAnnouncementResult::InvalidAttachment(
                    CreateAnnouncementErrorInvalidAttachment {
                        message: format!("Dataset {did} not found"),
                    },
                ));
            }
        }

        let target_dataset = dataset_reg
            .get_dataset_by_id(&self.announcements_dataset_id)
            .await
            .int_err()?;

        let announcement_id = uuid::Uuid::new_v4();

        let record = serde_json::json!({
            "op": u8::from(odf::metadata::OperationType::Append),
            "announcement_id": announcement_id.to_string(),
            "headline": headline,
            "body": body,
            "attachments": attachments,
            "molecule_access_level": molecule_access_level,
            "molecule_change_by": molecule_change_by,
        });

        push_ingest_use_case
            .execute(
                target_dataset,
                kamu_core::DataSource::Buffer(bytes::Bytes::from_owner(
                    record.to_string().into_bytes(),
                )),
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

        Ok(CreateAnnouncementResult::Success(
            CreateAnnouncementSuccess {
                announcement_id: announcement_id.to_string(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateProjectResult {
    Success(CreateProjectSuccess),
    Conflict(CreateProjectErrorConflict),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateProjectSuccess {
    pub project: v1::MoleculeProject,
}
#[ComplexObject]
impl CreateProjectSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateProjectErrorConflict {
    project: v1::MoleculeProject,
}
#[ComplexObject]
impl CreateProjectErrorConflict {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        format!(
            "Conflict with existing project {} ({})",
            self.project.ipnft_symbol, self.project.ipnft_uid,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateAnnouncementResult {
    Success(CreateAnnouncementSuccess),
    InvalidAttachment(CreateAnnouncementErrorInvalidAttachment),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementSuccess {
    /// ID of the newly-created announcement
    pub announcement_id: String,
}
#[ComplexObject]
impl CreateAnnouncementSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementErrorInvalidAttachment {
    message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorInvalidAttachment {
    async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
