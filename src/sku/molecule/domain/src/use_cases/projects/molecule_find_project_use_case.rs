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

use crate::{MoleculeGetDatasetError, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeFindProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProject>, MoleculeFindProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeFindProjectError {
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

impl From<MoleculeGetDatasetError> for MoleculeFindProjectError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        match e {
            MoleculeGetDatasetError::NotFound(err) => {
                MoleculeFindProjectError::NoProjectsDataset(err)
            }
            MoleculeGetDatasetError::Access(err) => MoleculeFindProjectError::Access(err),
            MoleculeGetDatasetError::Internal(err) => MoleculeFindProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
