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
use nonempty::NonEmpty;

use crate::{CollectionEntry, CollectionPath, ReadCheckedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ViewCollectionEntriesUseCase: Send + Sync {
    async fn execute(
        &self,
        collection_dataset: ReadCheckedDataset<'_>,
        as_of: Option<odf::Multihash>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        filter: Option<ExtraDataFieldsFilter>,
        pagination: Option<PaginationOpts>,
    ) -> Result<CollectionEntryListing, ViewCollectionEntriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Basic category filters.
///
/// Filters: ...
/// ```
/// [
///   ExtraDataFieldFilter {
///     field_name: "tags"
///     values: ["tag1, "tag2"]
///   },
///   ExtraDataFieldFilter {
///     field_name: "categories"
///     values: ["category1, "category2"]
///   }
/// ]
/// ```
/// ... are read as:
/// (`tag1` OR `tag2`) AND (`category1` OR `category2`).
pub type ExtraDataFieldsFilter = NonEmpty<ExtraDataFieldFilter>;

#[derive(Debug)]
pub struct ExtraDataFieldFilter {
    pub field_name: String,
    // TODO: Support another types (e.g. INTs)
    pub values: NonEmpty<String>,
}

pub type CollectionEntryListing = EntityPageListing<CollectionEntry>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ViewCollectionEntriesError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    UnknownExtraDataFieldFilterNames(#[from] UnknownExtraDataFieldFilterNamesError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Unknown extra data field filter names: {}",
    format_utils::format_collection(field_names)
)]
pub struct UnknownExtraDataFieldFilterNamesError {
    pub field_names: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
