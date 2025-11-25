// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use kamu_accounts::LoggedAccount;

use crate::MoleculeGetProjectsError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ViewMoleculeProjectsUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectListing, ViewMoleculeProjectsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct MoleculeProjectListing {
    pub records: Vec<serde_json::Value>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ViewMoleculeProjectsError {
    #[error(transparent)]
    NoProjectsDataset(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<MoleculeGetProjectsError> for ViewMoleculeProjectsError {
    fn from(e: MoleculeGetProjectsError) -> Self {
        match e {
            MoleculeGetProjectsError::NotFound(err) => {
                ViewMoleculeProjectsError::NoProjectsDataset(err)
            }
            MoleculeGetProjectsError::Access(err) => ViewMoleculeProjectsError::Access(err),
            MoleculeGetProjectsError::Internal(err) => ViewMoleculeProjectsError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
