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
use kamu_accounts::LoggedAccount;
use kamu_datasets::CollectionPath;

use crate::{MoleculeDataRoomEntry, MoleculeDenormalizeFileToDataRoom, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeUpdateDataRoomEntryUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        source_event_time: Option<DateTime<Utc>>,
        path: CollectionPath,
        reference: odf::DatasetID,
        expected_head: Option<odf::Multihash>,
        denormalized_file_info: MoleculeDenormalizeFileToDataRoom,
        content_text: Option<&str>,
    ) -> Result<MoleculeDataRoomEntry, MoleculeUpdateDataRoomEntryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeUpdateDataRoomEntryError {
    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    QuotaExceeded(#[from] kamu_accounts::QuotaError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
