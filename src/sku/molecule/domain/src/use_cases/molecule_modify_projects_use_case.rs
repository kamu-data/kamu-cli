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

use crate::{MoleculeGetProjectsError, MoleculeProjectEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeRestoreProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeRestoreProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeRestoreProjectError {
    #[error("Project with the same IPNFT UID or symbol already exists")]
    Conflict { project: MoleculeProjectEntity },

    #[error("Project with IPNFT UID {ipnft_uid} does not exist")]
    ProjectDoesNotExist { ipnft_uid: String },

    #[error(transparent)]
    NoProjectsDataset(#[from] odf::DatasetNotFoundError),

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

impl From<MoleculeGetProjectsError> for MoleculeRestoreProjectError {
    fn from(e: MoleculeGetProjectsError) -> Self {
        match e {
            MoleculeGetProjectsError::NotFound(err) => {
                MoleculeRestoreProjectError::NoProjectsDataset(err)
            }
            MoleculeGetProjectsError::Access(err) => MoleculeRestoreProjectError::Access(err),
            MoleculeGetProjectsError::Internal(err) => MoleculeRestoreProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeRemoveProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeRemoveProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeRemoveProjectError {
    #[error("Project with IPNFT UID {ipnft_uid} was not found")]
    ProjectNotFound { ipnft_uid: String },

    #[error(transparent)]
    NoProjectsDataset(#[from] odf::DatasetNotFoundError),

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

impl From<MoleculeGetProjectsError> for MoleculeRemoveProjectError {
    fn from(e: MoleculeGetProjectsError) -> Self {
        match e {
            MoleculeGetProjectsError::NotFound(err) => {
                MoleculeRemoveProjectError::NoProjectsDataset(err)
            }
            MoleculeGetProjectsError::Access(err) => MoleculeRemoveProjectError::Access(err),
            MoleculeGetProjectsError::Internal(err) => MoleculeRemoveProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
