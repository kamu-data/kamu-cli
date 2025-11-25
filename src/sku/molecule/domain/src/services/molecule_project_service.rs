// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::LoggedAccount;
use kamu_core::ResolvedDataset;
use kamu_core::auth::DatasetAction;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectService: Send + Sync {
    async fn get_projects_dataset(
        &self,
        molecule_account_name: &odf::AccountName,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<ResolvedDataset, MoleculeGetProjectsError>;

    async fn get_projects_data_frame(
        &self,
        molecule_subject: &LoggedAccount,
        action: DatasetAction,
        create_if_not_exist: bool,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>), MoleculeGetProjectsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeGetProjectsError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
