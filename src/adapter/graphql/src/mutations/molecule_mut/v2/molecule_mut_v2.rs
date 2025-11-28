// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::{
    MoleculeFindProjectError,
    MoleculeFindProjectUseCase,
    MoleculeRemoveProjectError,
    MoleculeRemoveProjectUseCase,
    MoleculeRestoreProjectError,
    MoleculeRestoreProjectUseCase,
};

use crate::molecule::molecule_subject;
use crate::mutations::molecule_mut::v1;
use crate::mutations::molecule_mut::v2::{MoleculeProjectMutV2, MoleculeProjectMutationResultV2};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct MoleculeMutV2 {
    v1: v1::MoleculeMutV1,
}

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
    ) -> Result<v1::CreateProjectResult> {
        self.v1
            .create_project(ctx, ipnft_symbol, ipnft_uid, ipnft_address, ipnft_token_id)
            .await
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
        let use_case = from_catalog_n!(ctx, dyn MoleculeRemoveProjectUseCase);

        let project = use_case
            .execute(&molecule_subject, ipnft_uid.clone())
            .await
            .map_err(|err| map_remove_error(err, &ipnft_uid))?;

        Ok(MoleculeProjectMutationResultV2::from_entity(project))
    }

    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_enable_project, skip_all, fields(?ipnft_uid))]
    async fn enable_project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectMutationResultV2> {
        let molecule_subject = molecule_subject(ctx)?;
        let use_case = from_catalog_n!(ctx, dyn MoleculeRestoreProjectUseCase);

        let project = use_case
            .execute(&molecule_subject, ipnft_uid.clone())
            .await
            .map_err(|err| map_restore_error(err, &ipnft_uid))?;

        Ok(MoleculeProjectMutationResultV2::from_entity(project))
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let molecule_find_project = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);
        let maybe_project_entity = molecule_find_project
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

fn map_remove_error(err: MoleculeRemoveProjectError, ipnft_uid: &str) -> GqlError {
    match err {
        MoleculeRemoveProjectError::ProjectNotFound { .. } => {
            GqlError::gql(format!("Project {ipnft_uid} not found"))
        }
        MoleculeRemoveProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeRemoveProjectError::Access(e) => GqlError::Access(e),
        MoleculeRemoveProjectError::Internal(e) => e.into(),
    }
}

fn map_restore_error(err: MoleculeRestoreProjectError, ipnft_uid: &str) -> GqlError {
    match err {
        MoleculeRestoreProjectError::Conflict { project } => {
            let conflict_uid = project.ipnft_uid.clone();
            let conflict_symbol = project.ipnft_symbol.clone();
            GqlError::gql_extended(
                format!("Project {ipnft_uid} conflicts with existing entries"),
                |ext| {
                    ext.set("conflict_ipnft_uid", conflict_uid);
                    ext.set("conflict_ipnft_symbol", conflict_symbol);
                },
            )
        }
        MoleculeRestoreProjectError::ProjectDoesNotExist { .. } => {
            GqlError::gql(format!("No historical entries for project {ipnft_uid}"))
        }
        MoleculeRestoreProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
        MoleculeRestoreProjectError::Access(e) => GqlError::Access(e),
        MoleculeRestoreProjectError::Internal(e) => e.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
