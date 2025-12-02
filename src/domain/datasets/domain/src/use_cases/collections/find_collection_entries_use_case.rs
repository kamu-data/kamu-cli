// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{CollectionEntry, CollectionPath, ReadCheckedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FindCollectionEntriesUseCase: Send + Sync {
    async fn execute_find_by_path(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        path: CollectionPath,
    ) -> Result<Option<CollectionEntry>, FindCollectionEntryUseCaseError>;

    async fn execute_find_multi_by_refs(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        refs: &[&odf::DatasetID],
    ) -> Result<Vec<CollectionEntry>, FindCollectionEntryUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum FindCollectionEntryUseCaseError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
