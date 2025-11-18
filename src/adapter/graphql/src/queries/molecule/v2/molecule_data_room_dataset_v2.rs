// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use url::Url;

use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeAccessLevelV2,
    MoleculeCategoryV2,
    MoleculeProjectV2,
    MoleculeTagV2,
};
use crate::queries::{Dataset, FileVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn entries(
        &self,
        _ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeDataRoomEntryV2Connection> {
        let _ = path_prefix;
        let _ = max_depth;
        let _ = page;
        let _ = per_page;

        // TODO: implement
        Ok(MoleculeDataRoomEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
    async fn entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<MoleculeDataRoomEntryV2> {
        let _ = path;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntryV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntryV2 {
    #[expect(clippy::unused_async)]
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn path(&self, _ctx: &Context<'_>) -> Result<CollectionPath> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    #[graphql(name = "ref")]
    async fn reference(&self, _ctx: &Context<'_>) -> Result<DatasetID<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Access the linked core Dataset
    async fn as_dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Strongly typed [`MoleculeVersionedFileV2`] object
    async fn as_versioned_file(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileV2> {
        todo!()
    }
}

page_based_connection!(
    MoleculeDataRoomEntryV2,
    MoleculeDataRoomEntryV2Connection,
    MoleculeDataRoomEntryV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileV2 {
    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn version(&self, _ctx: &Context<'_>) -> Result<FileVersion> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_hash(&self, _ctx: &Context<'_>) -> Result<Multihash<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_length(&self, _ctx: &Context<'_>) -> Result<usize> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    // TODO: typing
    async fn content_type(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevelV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<MoleculeCategoryV2>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<MoleculeTagV2>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_url(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileContentUrlV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: maybe just reuse from v1?
#[derive(SimpleObject)]
pub struct MoleculeVersionedFileContentUrlV2 {
    pub url: Url,
    pub headers: HashMap<String, String>,
    // TODO: typing
    pub method: String,
    pub expires_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
