// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ErrorIntoInternal;
use kamu::domain;
use kamu_core::{PushIngestDataError, PushIngestError};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use kamu_molecule_domain::{
    MoleculeCreateProjectError,
    MoleculeCreateProjectUseCase,
    MoleculeFindProjectError,
    MoleculeFindProjectUseCase,
};
use time_source::SystemTimeSource;

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
        if ipnft_uid != format!("{ipnft_address}_{}", ipnft_token_id.as_ref()) {
            return Err(Error::new("Inconsistent ipnft info").into());
        }

        let molecule_subject = molecule_subject(ctx)?;

        let (time_source, create_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeCreateProjectUseCase);

        let project = match create_project_uc
            .execute(
                &molecule_subject,
                Some(time_source.now()),
                ipnft_symbol,
                ipnft_uid,
                ipnft_address,
                ipnft_token_id.as_ref().clone(),
            )
            .await
        {
            Ok(project_entity) => v1::MoleculeProject::new(project_entity),
            Err(MoleculeCreateProjectError::Conflict { project }) => {
                let project = v1::MoleculeProject::new(project);
                return Ok(CreateProjectResult::Conflict(CreateProjectErrorConflict {
                    project,
                }));
            }
            Err(MoleculeCreateProjectError::Access(e)) => return Err(GqlError::Access(e)),
            Err(e @ MoleculeCreateProjectError::Internal(_)) => {
                return Err(e.int_err().into());
            }
        };

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

        let find_project_uc = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);

        let maybe_project = find_project_uc
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                MoleculeFindProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeFindProjectError::Access(e) => GqlError::Access(e),
                e @ MoleculeFindProjectError::Internal(_) => e.int_err().into(),
            })
            .map(|opt| opt.map(MoleculeProjectMut::from_entity))?;

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
    pub fn from_entity(entity: kamu_molecule_domain::MoleculeProject) -> Self {
        Self {
            account_id: entity.account_id,
            data_room_dataset_id: entity.data_room_dataset_id,
            announcements_dataset_id: entity.announcements_dataset_id,
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
        let (dataset_reg, push_ingest_uc) =
            from_catalog_n!(ctx, dyn DatasetRegistry, dyn domain::PushIngestDataUseCase);

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
            // V1:
            "op": u8::from(odf::metadata::OperationType::Append),
            "announcement_id": announcement_id.to_string(),
            "headline": headline,
            "body": body,
            "attachments": attachments,
            "molecule_access_level": molecule_access_level,
            "molecule_change_by": molecule_change_by,
            // V2:
            "categories": [],
            "tags": [],
        });

        match push_ingest_uc
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
                    skip_quota_check: false,
                },
                None,
            )
            .await
        {
            Ok(_) => Ok(CreateAnnouncementResult::Success(
                CreateAnnouncementSuccess {
                    announcement_id: announcement_id.to_string(),
                },
            )),
            Err(PushIngestDataError::Execution(PushIngestError::QuotaExceeded(err))) => Ok(
                CreateAnnouncementResult::QuotaExceeded(CreateAnnouncementErrorQuotaExceeded {
                    message: err.to_string(),
                }),
            ),
            Err(other) => Err(other.int_err().into()),
        }
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
            self.project.entity.ipnft_symbol, self.project.entity.ipnft_uid,
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
    QuotaExceeded(CreateAnnouncementErrorQuotaExceeded),
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
    pub(crate) message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorInvalidAttachment {
    async fn is_success(&self) -> bool {
        false
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAnnouncementErrorQuotaExceeded {
    pub(crate) message: String,
}
#[ComplexObject]
impl CreateAnnouncementErrorQuotaExceeded {
    async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
