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
pub trait CreateMoleculeProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: num_bigint::BigInt,
    ) -> Result<MoleculeProjectEntity, CreateMoleculeProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CreateMoleculeProjectError {
    #[error("Project with the same IPNFT UID or symbol already exists")]
    Conflict { project: MoleculeProjectEntity },

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<MoleculeGetProjectsError> for CreateMoleculeProjectError {
    fn from(e: MoleculeGetProjectsError) -> Self {
        match e {
            MoleculeGetProjectsError::NotFound(err) => {
                unreachable!("Projects dataset should be created if not exist: {}", err)
            }
            MoleculeGetProjectsError::Access(err) => CreateMoleculeProjectError::Access(err),
            MoleculeGetProjectsError::Internal(err) => CreateMoleculeProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
