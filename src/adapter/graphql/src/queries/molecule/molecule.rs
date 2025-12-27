// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{v1, v2};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct Molecule {
    v1: v1::MoleculeV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Molecule {
    /// Looks up the project.
    ///
    /// Shortcut for `v1 { project }`.
    #[graphql(deprecation = "Use `v2 { project }` instead.")]
    #[tracing::instrument(level = "info", name = Molecule_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<v1::MoleculeProject>> {
        self.v1.project(ctx, ipnft_uid).await
    }

    /// List the registered projects.
    ///
    /// Shortcut for `v1 { projects }`.
    #[graphql(deprecation = "Use `v2 { projects }` instead.")]
    #[tracing::instrument(level = "info", name = Molecule_projects, skip_all)]
    async fn projects(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<v1::MoleculeProjectConnection> {
        self.v1.projects(ctx, page, per_page).await
    }

    /// Latest activity events across all projects in reverse chronological
    /// order.
    ///
    /// Shortcut for `v1 { activity }`.
    #[graphql(deprecation = "Use `v2 { activity }` instead.")]
    #[tracing::instrument(level = "info", name = Molecule_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<v1::MoleculeProjectEventConnection> {
        self.v1.activity(ctx, page, per_page).await
    }

    /// 1-st Molecule API version (query).
    #[graphql(deprecation = "Use `v2` instead")]
    async fn v1(&self) -> v1::MoleculeV1 {
        v1::MoleculeV1
    }

    /// 2-nd Molecule API version (query).
    async fn v2(&self) -> v2::MoleculeV2 {
        v2::MoleculeV2
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
