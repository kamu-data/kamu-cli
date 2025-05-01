// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollectionMut {
    state: DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl CollectionMut {
    #[graphql(skip)]
    pub fn new(state: DatasetRequestState) -> Self {
        Self { state }
    }

    /// Links new entry to this collection
    #[tracing::instrument(level = "info", name = CollectionMut_add_entry, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn add_entry(&self, entry: CollectionEntryInput) -> CollectionUpdateResult {
        todo!()
    }

    /// Moves or rename an entry
    #[tracing::instrument(level = "info", name = CollectionMut_move_entry, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn move_entry(&self, path_from: String, path_to: String) -> CollectionUpdateResult {
        todo!()
    }

    /// Remove an entry from this collection
    #[tracing::instrument(level = "info", name = CollectionMut_unlink_entry, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn unlink_entry(&self, path: String) -> CollectionUpdateResult {
        todo!()
    }

    /// Execute multiple add / move / unlink operations as a single transaction
    #[tracing::instrument(level = "info", name = CollectionMut_batch_update, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn batch_update(
        &self,
        operations: Vec<CollectionUpdateInput>,
    ) -> CollectionUpdateResult {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct CollectionEntryInput {
    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    path: String,

    /// DID of the linked dataset
    did: DatasetID<'static>,

    /// Json object containing extra column values
    extra_data: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInput {
    add: Option<CollectionUpdateInputAdd>,
    r#move: Option<CollectionUpdateInputMove>,
    unlink: Option<CollectionUpdateInputUnlink>,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputAdd {
    entry: CollectionEntryInput,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputMove {
    path_from: String,
    path_to: String,
}

#[derive(InputObject, Debug)]
pub struct CollectionUpdateInputUnlink {
    path: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum CollectionUpdateResult {
    Success(CollectionUpdateSuccess),
    NotFound(CollectionUpdateErrorNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateSuccess {
    pub new_head: Multihash<'static>,
}
#[ComplexObject]
impl CollectionUpdateSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CollectionUpdateErrorNotFound {
    pub path: String,
}
#[ComplexObject]
impl CollectionUpdateErrorNotFound {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> String {
        format!("Path {} does not exist", self.path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
