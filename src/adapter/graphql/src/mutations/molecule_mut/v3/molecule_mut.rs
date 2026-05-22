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
use crate::mutations::molecule_mut::v3::{MoleculeProjectMut, MoleculeProjectMutationResult};
use crate::prelude::*;
use crate::queries::molecule::v3::{MoleculeProject, OclId, Symbol};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: A top-level type already takes MoleculeMut GQL name
pub struct MoleculeMutV3;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV3 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV3_create_project, skip_all, fields(?ocl_id, ?symbol))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ocl_id: OclId<'_>,
        symbol: Symbol<'_>,
    ) -> Result<CreateProjectResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (time_source, create_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeCreateProjectUseCase);

        let project = match create_project_uc
            .execute(
                &molecule_subject,
                Some(time_source.now()),
                ocl_id.into(),
                symbol.into(),
            )
            .await
        {
            Ok(project_entity) => MoleculeProject::new(project_entity),
            Err(MoleculeCreateProjectError::Conflict { project }) => {
                let project = MoleculeProject::new(project);
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
    #[tracing::instrument(level = "info", name = MoleculeMutV3_disable_project, skip_all, fields(?ocl_id))]
    async fn disable_project(
        &self,
        ctx: &Context<'_>,
        ocl_id: OclId<'_>,
    ) -> Result<MoleculeProjectMutationResult> {
        let molecule_subject = molecule_subject(ctx)?;
        let (time_source, disable_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeDisableProjectUseCase);

        let ocl_id: kamu_molecule_domain::OclId = ocl_id.into();
        let project = disable_project_uc
            .execute(&molecule_subject, Some(time_source.now()), ocl_id.clone())
            .await
            .map_err(|err| map_disable_error(err, &ocl_id))?;

        Ok(MoleculeProjectMutationResult::from_entity(
            project,
            "Project disabled successfully".to_string(),
        ))
    }

    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV3_enable_project, skip_all, fields(?ocl_id))]
    async fn enable_project(
        &self,
        ctx: &Context<'_>,
        ocl_id: OclId<'_>,
    ) -> Result<MoleculeProjectMutationResult> {
        let molecule_subject = molecule_subject(ctx)?;
        let (time_source, enable_project_uc) =
            from_catalog_n!(ctx, dyn SystemTimeSource, dyn MoleculeEnableProjectUseCase);

        let ocl_id: kamu_molecule_domain::OclId = ocl_id.into();
        let project = enable_project_uc
            .execute(&molecule_subject, Some(time_source.now()), ocl_id.clone())
            .await
            .map_err(|err| map_enable_error(err, &ocl_id))?;

        Ok(MoleculeProjectMutationResult::from_entity(
            project,
            "Project enabled successfully".to_string(),
        ))
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV3_project, skip_all, fields(?ocl_id))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ocl_id: OclId<'_>,
    ) -> Result<Option<MoleculeProjectMut>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_project_uc = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);

        let maybe_project_entity = find_project_uc
            .execute(&molecule_subject, ocl_id.into())
            .await
            .map_err(|e| match e {
                MoleculeFindProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeFindProjectError::Access(e) => GqlError::Access(e),
                e @ MoleculeFindProjectError::Internal(_) => e.int_err().into(),
            })?;

        Ok(maybe_project_entity.map(MoleculeProjectMut::from_entity))
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
    pub project: MoleculeProject,
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
    project: MoleculeProject,
}
#[ComplexObject]
impl CreateProjectErrorConflict {
    async fn is_success(&self) -> bool {
        false
    }
    async fn message(&self) -> String {
        format!(
            "Conflict with existing project {} ({})",
            self.project.entity.symbol, self.project.entity.ocl_id,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_disable_error(
    err: MoleculeDisableProjectError,
    ocl_id: &kamu_molecule_domain::OclId,
) -> GqlError {
    match err {
        MoleculeDisableProjectError::ProjectNotFound(_) => {
            GqlError::gql(format!("Project [{ocl_id}] not found"))
        }
        MoleculeDisableProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeDisableProjectError::Access(e) => GqlError::Access(e),
        MoleculeDisableProjectError::Internal(e) => e.into(),
    }
}

fn map_enable_error(
    err: MoleculeEnableProjectError,
    ocl_id: &kamu_molecule_domain::OclId,
) -> GqlError {
    match err {
        MoleculeEnableProjectError::ProjectNotFound(_) => {
            GqlError::gql(format!("No historical entries for project [{ocl_id}]"))
        }
        MoleculeEnableProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeEnableProjectError::Access(e) => GqlError::Access(e),
        MoleculeEnableProjectError::Internal(e) => e.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
