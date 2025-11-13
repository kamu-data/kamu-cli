// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{CreateAnnouncementResult, CreateProjectResult, MoleculeMutV1};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct MoleculeMutV2 {
    v1: MoleculeMutV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV2 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: U256,
    ) -> Result<CreateProjectResult> {
        self.v1
            .create_project(ctx, ipnft_symbol, ipnft_uid, ipnft_address, ipnft_token_id)
            .await
    }

    /// Retracts a project from the `projects` dataset.
    /// History of this project existing will be preserved,
    /// its symbol will remain reserved, data will remain intact,
    /// but the project will no longer appear in the listing.
    #[tracing::instrument(level = "info", name = MoleculeMutV2_remove_project, skip_all, fields(?ipnft_uid))]
    async fn remove_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<RemoveProjectResult> {
        let _ = ipnft_uid;
        todo!()
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        let _ = ipnft_uid;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutV2 {
    /// Creates an announcement record for the project
    #[tracing::instrument(level = "info", name = MoleculeProjectMutV2_create_announcement, skip_all)]
    async fn create_announcement(
        &self,
        _ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<String>>,
        molecule_access_level: String,
        molecule_change_by: String,
    ) -> Result<CreateAnnouncementResult> {
        let _ = headline;
        let _ = body;
        let _ = attachments;
        let _ = molecule_access_level;
        let _ = molecule_change_by;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type RemoveProjectResult = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
