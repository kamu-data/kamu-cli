// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::molecule_mut::{v1, v2};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct MoleculeMut {
    molecule_mut_v1: v1::MoleculeMutV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMut {
    /// Shortcut for `v1 { create_project() }`.
    #[graphql(
        guard = "LoggedInGuard",
        deprecation = "Use `v2 { create_project() }` instead."
    )]
    #[tracing::instrument(level = "info", name = MoleculeMut_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: U256,
    ) -> Result<v1::CreateProjectResult> {
        self.molecule_mut_v1
            .create_project(ctx, ipnft_symbol, ipnft_uid, ipnft_address, ipnft_token_id)
            .await
    }

    /// Looks up the project
    ///
    /// Shortcut for `v1 { project() }`.
    #[graphql(deprecation = "Use `v2 { project }` instead.")]
    #[tracing::instrument(level = "info", name = MoleculeMut_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<v1::MoleculeProjectMut>> {
        self.molecule_mut_v1.project(ctx, ipnft_uid).await
    }

    /// 1-st Molecule API version (mutation).
    #[graphql(deprecation = "Use `v2` instead")]
    async fn v1(&self) -> v1::MoleculeMutV1 {
        v1::MoleculeMutV1
    }

    /// 2-nd Molecule API version (mutation).
    async fn v2(&self) -> v2::MoleculeMutV2 {
        v2::MoleculeMutV2::default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
