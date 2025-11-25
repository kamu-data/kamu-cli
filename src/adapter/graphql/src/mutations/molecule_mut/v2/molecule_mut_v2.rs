// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::{FindMoleculeProjectError, FindMoleculeProjectUseCase};

use crate::molecule::molecule_subject;
use crate::mutations::molecule_mut::v1;
use crate::mutations::molecule_mut::v2::MoleculeProjectMutV2;
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
    #[tracing::instrument(level = "info", name = MoleculeMutV2_disable_project, skip_all, fields(?ipnft_uid))]
    async fn disable_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeDisableProjectResultV2> {
        let _ = ipnft_uid;
        todo!()
    }

    #[tracing::instrument(level = "info", name = MoleculeMutV2_enable_project, skip_all, fields(?ipnft_uid))]
    async fn enable_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeEnableProjectResultV2> {
        let _ = ipnft_uid;
        todo!()
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_molecule_project = from_catalog_n!(ctx, dyn FindMoleculeProjectUseCase);
        let maybe_project_json = find_molecule_project
            .execute(molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                FindMoleculeProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                FindMoleculeProjectError::Access(e) => GqlError::Access(e),
                FindMoleculeProjectError::Internal(e) => GqlError::Gql(e.into()),
            })?;

        let maybe_project_v2 = maybe_project_json
            .map(MoleculeProjectMutV2::from_json)
            .transpose()?;
        Ok(maybe_project_v2)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDisableProjectResultV2 {
    pub dummy: String,
}

#[derive(SimpleObject)]
pub struct MoleculeEnableProjectResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
