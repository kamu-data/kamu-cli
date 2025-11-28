// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityPageListing, PaginationOpts};
use internal_error::{ErrorIntoInternal, InternalError};

use crate::{MoleculeDataRoomActivityEntity, MoleculeGetDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewGlobalDataRoomActivitiesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeDataRoomActivityListing = EntityPageListing<MoleculeDataRoomActivityEntity>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewDataRoomActivitiesError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<MoleculeGetDatasetError> for MoleculeViewDataRoomActivitiesError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        use MoleculeGetDatasetError as E;
        match e {
            E::Access(e) => e.into(),
            E::NotFound(_) | E::Internal(_) => e.int_err().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
