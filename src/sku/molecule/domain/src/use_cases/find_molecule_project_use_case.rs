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

use crate::MoleculeGetProjectsError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FindMoleculeProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<Option<serde_json::Value>, FindMoleculeProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum FindMoleculeProjectError {
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

impl From<MoleculeGetProjectsError> for FindMoleculeProjectError {
    fn from(e: MoleculeGetProjectsError) -> Self {
        match e {
            MoleculeGetProjectsError::NotFound(err) => {
                FindMoleculeProjectError::NoProjectsDataset(err)
            }
            MoleculeGetProjectsError::Access(err) => FindMoleculeProjectError::Access(err),
            MoleculeGetProjectsError::Internal(err) => FindMoleculeProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
