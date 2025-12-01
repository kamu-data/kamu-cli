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
use kamu_datasets::CollectionPath;

use crate::{MoleculeDataRoomEntry, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewProjectDataRoomEntriesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        // TODO: filters
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomEntriesListing, MoleculeViewProjectDataRoomError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeDataRoomEntriesListing = EntityPageListing<MoleculeDataRoomEntry>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewProjectDataRoomError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
