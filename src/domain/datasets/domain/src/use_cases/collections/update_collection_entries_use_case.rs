// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use odf::dataset::RefCASError;
use thiserror::Error;

use crate::{CollectionPath, ExtraDataFields, WriteCheckedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateCollectionEntriesUseCase: Send + Sync {
    async fn execute(
        &self,
        collection_dataset: WriteCheckedDataset<'_>,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub enum CollectionUpdateOperation {
    Add(CollectionEntryUpdate),
    Move(CollectionEntryMove),
    Remove(CollectionEntryRemove),
}

impl CollectionUpdateOperation {
    pub fn add(
        path: CollectionPath,
        reference: odf::DatasetID,
        extra_data: ExtraDataFields,
    ) -> Self {
        Self::Add(CollectionEntryUpdate {
            path,
            reference,
            extra_data,
        })
    }

    pub fn r#move(
        path_from: CollectionPath,
        path_to: CollectionPath,
        extra_data: Option<ExtraDataFields>,
    ) -> Self {
        Self::Move(CollectionEntryMove {
            path_from,
            path_to,
            extra_data,
        })
    }

    pub fn remove(path: CollectionPath) -> Self {
        Self::Remove(CollectionEntryRemove { path })
    }
}

#[derive(Clone)]
pub struct CollectionEntryUpdate {
    pub path: CollectionPath,
    pub reference: odf::DatasetID,
    pub extra_data: ExtraDataFields,
}

#[derive(Clone)]
pub struct CollectionEntryRemove {
    pub path: CollectionPath,
}

#[derive(Clone)]
pub struct CollectionEntryMove {
    pub path_from: CollectionPath,
    pub path_to: CollectionPath,
    pub extra_data: Option<ExtraDataFields>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum UpdateCollectionEntriesResult {
    Success(UpdateCollectionEntriesSuccess),
    UpToDate,
    NotFound(CollectionEntryNotFound),
}

#[derive(Debug)]
pub struct UpdateCollectionEntriesSuccess {
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
}

#[derive(Debug)]
pub struct CollectionEntryNotFound {
    pub path: CollectionPath,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateCollectionEntriesUseCaseError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    RefCASFailed(#[from] RefCASError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
