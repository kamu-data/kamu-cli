// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::CollectionPath;

use crate::{MoleculeProject, MoleculeUpdateProjectDataRoomEntryResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeMoveProjectDataRoomEntryUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        path_from: CollectionPath,
        path_to: CollectionPath,
        expected_head: Option<odf::Multihash>,
    ) -> Result<MoleculeUpdateProjectDataRoomEntryResult, MoleculeMoveProjectDataRoomEntryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeMoveProjectDataRoomEntryError {
    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
