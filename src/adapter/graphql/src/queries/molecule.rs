// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: breakdown to smaller files after API freeze stage.
// TODO: use dir tree structure e.g. molecule/{v1,v2}/molecule_project_v2.rs.

use kamu_accounts::{CurrentAccountSubject, LoggedAccount};

use super::{
    MoleculeProject,
    MoleculeProjectConnection,
    MoleculeProjectEventConnection,
    MoleculeV1,
    MoleculeV2,
};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct Molecule {
    v1: MoleculeV1,
}

impl Molecule {
    // Public only for tests
    pub fn dataset_snapshot_projects_v1(alias: odf::DatasetAlias) -> odf::DatasetSnapshot {
        MoleculeV1::dataset_snapshot_projects(alias)
    }
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
    ) -> Result<Option<MoleculeProject>> {
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
    ) -> Result<MoleculeProjectConnection> {
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
    ) -> Result<MoleculeProjectEventConnection> {
        self.v1.activity(ctx, page, per_page).await
    }

    /// 1-st Molecule API version (query).
    #[graphql(deprecation = "Use `v2` instead")]
    async fn v1(&self) -> MoleculeV1 {
        MoleculeV1
    }

    /// 2-nd Molecule API version (query).
    async fn v2(&self) -> MoleculeV2 {
        MoleculeV2
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_ORG_ACCOUNTS: [&str; 2] = ["molecule", "molecule.dev"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn molecule_subject(ctx: &Context<'_>) -> Result<LoggedAccount> {
    // Check auth
    let subject = from_catalog_n!(ctx, CurrentAccountSubject);
    let subject_molecule = match subject.as_ref() {
        CurrentAccountSubject::Logged(subj)
            if MOLECULE_ORG_ACCOUNTS.contains(&subj.account_name.as_str()) =>
        {
            subj
        }
        _ => {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                format!(
                    "Only accounts {} can provision projects",
                    MOLECULE_ORG_ACCOUNTS
                        .iter()
                        .map(|account_name| format!("'{account_name}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .as_str()
                .into(),
            )));
        }
    };
    Ok(subject_molecule.clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
