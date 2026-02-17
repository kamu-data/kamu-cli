// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityPageListing, PaginationOpts};
use internal_error::InternalError;
use kamu_accounts::LoggedAccount;
use kamu_datasets::CollectionPath;

use crate::{MoleculeDataRoomEntriesFilters, MoleculeDataRoomEntry, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewDataRoomEntriesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewDataRoomEntriesMode,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewDataRoomEntriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeViewDataRoomEntriesMode {
    LatestProjection,
    LatestSource,
    Historical(odf::Multihash),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeDataRoomEntriesListing = EntityPageListing<MoleculeDataRoomEntry>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewDataRoomEntriesError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
