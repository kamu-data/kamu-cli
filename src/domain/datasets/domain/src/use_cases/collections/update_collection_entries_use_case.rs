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

use crate::{ExtraDataFields, WriteCheckedDataset};

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
    pub fn add(path: String, reference: odf::DatasetID, extra_data: ExtraDataFields) -> Self {
        Self::Add(CollectionEntryUpdate {
            path,
            reference,
            extra_data,
        })
    }

    pub fn r#move(path_from: String, path_to: String, extra_data: Option<ExtraDataFields>) -> Self {
        Self::Move(CollectionEntryMove {
            path_from,
            path_to,
            extra_data,
        })
    }

    pub fn remove(path: String) -> Self {
        Self::Remove(CollectionEntryRemove { path })
    }
}

#[derive(Clone)]
pub struct CollectionEntryUpdate {
    pub path: String,
    pub reference: odf::DatasetID,
    pub extra_data: ExtraDataFields,
}

#[derive(Clone)]
pub struct CollectionEntryRemove {
    pub path: String,
}

#[derive(Clone)]
pub struct CollectionEntryMove {
    pub path_from: String,
    pub path_to: String,
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
    pub path: String,
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
