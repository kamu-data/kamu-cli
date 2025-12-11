// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth;
use kamu_datasets::ResolvedDataset;
use kamu_molecule_domain::MoleculeGetDatasetError;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeAnnouncementsDatasetService: Send + Sync {
    async fn get_global_announcements_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError>;

    async fn get_global_announcements_data_frame(
        &self,
        molecule_account_name: &odf::AccountName,
        action: auth::DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError>;

    async fn get_project_announcements_dataset(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, MoleculeGetDatasetError>;

    async fn get_project_announcements_data_frame(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        action: auth::DatasetAction,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
