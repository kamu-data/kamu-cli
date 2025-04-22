// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Collection {
    state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Collection {
    #[graphql(skip)]
    pub fn new(state: DatasetRequestStateWithOwner) -> Self {
        Self { state }
    }

    /// Returns flattened list of entries in the collection at latest or
    /// specified point in time
    #[tracing::instrument(level = "info", name = Collection_list_entries, skip_all)]
    pub async fn list_entries(
        &self,
        path_prefix: Option<String>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> CollectionEntryConnection {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct CollectionEntry {
    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    path: String,

    /// DID of the linked dataset
    did: DatasetID<'static>,

    /// Json object containing extra column values
    extra_data: serde_json::Value,
}

page_based_connection!(
    CollectionEntry,
    CollectionEntryConnection,
    CollectionEntryEdge
);
