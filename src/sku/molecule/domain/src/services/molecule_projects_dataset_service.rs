// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::LoggedAccount;
use kamu_core::auth;
use kamu_datasets::ResolvedDataset;
use odf::utils::data::DataFrameExt;

use crate::{MoleculeGetDatasetError, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectsDatasetService: Send + Sync {
    async fn get_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError>;

    /// Returns raw ledger data without projection
    async fn get_projects_raw_ledger_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError>;

    /// Returns the projected changelog
    async fn get_projects_changelog_projection_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError>;

    async fn get_project_changelog_entry(
        &self,
        molecule_subject: &LoggedAccount,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
        ipnft_uid: &str,
    ) -> Result<(ResolvedDataset, Option<MoleculeProject>), MoleculeGetDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
