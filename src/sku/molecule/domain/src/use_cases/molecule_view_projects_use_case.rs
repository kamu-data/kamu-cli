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

use crate::{MoleculeGetDatasetError, MoleculeProjectEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewProjectsUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectListing, MoleculeViewProjectsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct MoleculeProjectListing {
    pub projects: Vec<MoleculeProjectEntity>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewProjectsError {
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

impl From<MoleculeGetDatasetError> for MoleculeViewProjectsError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        match e {
            MoleculeGetDatasetError::NotFound(err) => {
                MoleculeViewProjectsError::NoProjectsDataset(err)
            }
            MoleculeGetDatasetError::Access(err) => MoleculeViewProjectsError::Access(err),
            MoleculeGetDatasetError::Internal(err) => MoleculeViewProjectsError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
