// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_accounts::{AccountDuplicateField, LoggedAccount};

use crate::{MoleculeGetDatasetError, MoleculeProject, OclId, Symbol};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeCreateProjectUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        source_event_time: Option<DateTime<Utc>>,
        ocl_id: OclId,
        symbol: Symbol,
    ) -> Result<MoleculeProject, MoleculeCreateProjectError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeCreateProjectError {
    #[error("Project with the same OCL ID or symbol already exists")]
    ConflictProject { project: MoleculeProject },

    #[error("Project account '{project_account_name}' already exists")]
    ConflictAccount {
        project_account_name: odf::AccountName,
        account_duplicate_field: AccountDuplicateField,
    },

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<MoleculeGetDatasetError> for MoleculeCreateProjectError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        match e {
            MoleculeGetDatasetError::NotFound(err) => {
                unreachable!("Projects dataset should be created if not exist: {}", err)
            }
            MoleculeGetDatasetError::Access(err) => MoleculeCreateProjectError::Access(err),
            MoleculeGetDatasetError::Internal(err) => MoleculeCreateProjectError::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
