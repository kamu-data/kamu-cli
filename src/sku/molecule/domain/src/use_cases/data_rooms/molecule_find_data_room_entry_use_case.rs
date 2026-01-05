// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::BatchLookup;
use internal_error::InternalError;
use kamu_datasets::CollectionPath;

use crate::{MoleculeDataRoomEntry, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeFindDataRoomEntryUseCase: Send + Sync {
    async fn execute_find_by_path(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<MoleculeDataRoomEntry>, MoleculeFindDataRoomEntryError>;

    async fn execute_find_by_ref(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        r#ref: &odf::DatasetID,
    ) -> Result<Option<MoleculeDataRoomEntry>, MoleculeFindDataRoomEntryError>;

    async fn execute_find_by_refs(
        &self,
        molecule_project: &MoleculeProject,
        as_of: Option<odf::Multihash>,
        refs: &[&odf::DatasetID],
    ) -> Result<
        BatchLookup<MoleculeDataRoomEntry, odf::DatasetID, MoleculeFindDataRoomEntryError>,
        InternalError,
    >;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeFindDataRoomEntryError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
