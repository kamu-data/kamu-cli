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

use crate::{MoleculeGetDatasetError, MoleculeProjectEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeEnableProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeEnableProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeEnableProjectError {
    #[error("Project with the same IPNFT UID or symbol already exists")]
    Conflict { project: MoleculeProjectEntity },

    #[error(transparent)]
    ProjectNotFound(#[from] ProjectNotFoundError),

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

impl From<MoleculeGetDatasetError> for MoleculeEnableProjectError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        match e {
            MoleculeGetDatasetError::NotFound(err) => {
                MoleculeEnableProjectError::NoProjectsDataset(err)
            }
            MoleculeGetDatasetError::Access(err) => MoleculeEnableProjectError::Access(err),
            MoleculeGetDatasetError::Internal(err) => MoleculeEnableProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeDisableProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProjectEntity, MoleculeDisableProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeDisableProjectError {
    #[error(transparent)]
    ProjectNotFound(#[from] ProjectNotFoundError),

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

impl From<MoleculeGetDatasetError> for MoleculeDisableProjectError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        match e {
            MoleculeGetDatasetError::NotFound(err) => {
                MoleculeDisableProjectError::NoProjectsDataset(err)
            }
            MoleculeGetDatasetError::Access(err) => MoleculeDisableProjectError::Access(err),
            MoleculeGetDatasetError::Internal(err) => MoleculeDisableProjectError::Internal(err),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Project with IPNFT UID {ipnft_uid} was not found")]
pub struct ProjectNotFoundError {
    pub ipnft_uid: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
