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
use kamu_datasets::CollectionPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeUpdateDataRoomEntryResult {
    Success(MoleculeUpdateDataRoomEntrySuccess),
    UpToDate,
    EntryNotFound(CollectionPath),
}

#[derive(Debug)]
pub struct MoleculeUpdateDataRoomEntrySuccess {
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub inserted_records: Vec<(
        odf::metadata::OperationType,
        kamu_datasets::CollectionEntryRecord,
    )>,
    pub system_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeGetDatasetError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
