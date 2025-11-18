// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::Dataset;
use crate::queries::molecule::v2::{
    MoleculeAccessLevelV2,
    MoleculeAnnouncementID,
    MoleculeProjectV2,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementsDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn tail(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeAnnouncementEntryV2Connection> {
        let _ = page;
        let _ = per_page;
        Ok(MoleculeAnnouncementEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
    async fn by_id(
        &self,
        _ctx: &Context<'_>,
        id: MoleculeAnnouncementID,
    ) -> Result<MoleculeAnnouncementEntryV2> {
        let _ = id;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementEntryV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementEntryV2 {
    #[expect(clippy::unused_async)]
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn id(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementID> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn headline(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn body(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn attachments(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevelV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn change_by(&self, _ctx: &Context<'_>) -> Result<AccountID<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }
}

page_based_connection!(
    MoleculeAnnouncementEntryV2,
    MoleculeAnnouncementEntryV2Connection,
    MoleculeAnnouncementEntryV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
