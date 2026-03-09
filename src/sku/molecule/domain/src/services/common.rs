// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_core::QueryError;
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

pub trait MoleculeDatasetErrorExt {
    fn adapt<E>(self) -> E
    where
        E: From<odf::AccessError> + From<InternalError>;
}

impl MoleculeDatasetErrorExt for RebacDatasetRefUnresolvedError {
    fn adapt<E>(self) -> E
    where
        E: From<odf::AccessError> + From<InternalError>,
    {
        match self {
            RebacDatasetRefUnresolvedError::NotFound(e) => e.int_err().into(),
            RebacDatasetRefUnresolvedError::Access(e) => e.into(),
            RebacDatasetRefUnresolvedError::Internal(e) => e.into(),
        }
    }
}

impl MoleculeDatasetErrorExt for QueryError {
    fn adapt<E>(self) -> E
    where
        E: From<odf::AccessError> + From<InternalError>,
    {
        match self {
            QueryError::Access(e) => e.into(),
            QueryError::Internal(e) => e.into(),

            QueryError::BadQuery(_)
            | QueryError::DatasetNotFound(_)
            | QueryError::DatasetBlockNotFound(_) => self.int_err().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
