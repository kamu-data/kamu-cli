// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::*;
use time_source::SystemTimeSource;

use crate::molecule::molecule_subject;
use crate::mutations::molecule_mut::v2::{MoleculeProjectMutV2, MoleculeProjectMutationResultV2};
use crate::prelude::*;
use crate::queries::molecule::v2::MoleculeProjectV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeMutV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV2 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
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
            Ok(project_entity) => MoleculeProjectV2::new(project_entity),
            Err(MoleculeCreateProjectError::Conflict { project }) => {
                let project = MoleculeProjectV2::new(project);
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

    /// Retracts a project from the `projects` dataset.
    /// History of this project existing will be preserved,
    /// its symbol will remain reserved, data will remain intact,
    /// but the project will no longer appear in the listing.
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_disable_project, skip_all, fields(?ipnft_uid))]
    async fn disable_project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectMutationResultV2> {
        let molecule_subject = molecule_subject(ctx)?;
        let (time_source, disable_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeDisableProjectUseCase);

        let project = disable_project_uc
            .execute(
                &molecule_subject,
                Some(time_source.now()),
                ipnft_uid.clone(),
            )
            .await
            .map_err(|err| map_disable_error(err, &ipnft_uid))?;

        Ok(MoleculeProjectMutationResultV2::from_entity(
            project,
            "Project disabled successfully".to_string(),
        ))
    }

    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_enable_project, skip_all, fields(?ipnft_uid))]
    async fn enable_project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectMutationResultV2> {
        let molecule_subject = molecule_subject(ctx)?;
        let (time_source, enable_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeEnableProjectUseCase);

        let project = enable_project_uc
            .execute(
                &molecule_subject,
                Some(time_source.now()),
                ipnft_uid.clone(),
            )
            .await
            .map_err(|err| map_enable_error(err, &ipnft_uid))?;

        Ok(MoleculeProjectMutationResultV2::from_entity(
            project,
            "Project enabled successfully".to_string(),
        ))
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_project_uc = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);

        let maybe_project_entity = find_project_uc
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                MoleculeFindProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeFindProjectError::Access(e) => GqlError::Access(e),
                e @ MoleculeFindProjectError::Internal(_) => e.int_err().into(),
            })?;

        Ok(maybe_project_entity.map(MoleculeProjectMutV2::from_entity))
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
    pub project: MoleculeProjectV2,
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
    project: MoleculeProjectV2,
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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_disable_error(err: MoleculeDisableProjectError, ipnft_uid: &str) -> GqlError {
    match err {
        MoleculeDisableProjectError::ProjectNotFound(_) => {
            GqlError::gql(format!("Project {ipnft_uid} not found"))
        }
        MoleculeDisableProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeDisableProjectError::Access(e) => GqlError::Access(e),
        MoleculeDisableProjectError::Internal(e) => e.into(),
    }
}

fn map_enable_error(err: MoleculeEnableProjectError, ipnft_uid: &str) -> GqlError {
    match err {
        MoleculeEnableProjectError::ProjectNotFound(_) => {
            GqlError::gql(format!("No historical entries for project {ipnft_uid}"))
        }
        MoleculeEnableProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeEnableProjectError::Access(e) => GqlError::Access(e),
        MoleculeEnableProjectError::Internal(e) => e.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
