// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::prelude::*;
use crate::queries::Account;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2Connection,
    MoleculeActivityFiltersV2,
    MoleculeAnnouncementsDatasetV2,
    MoleculeDataRoomDatasetV2,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct MoleculeProjectV2 {
    #[expect(dead_code)]
    #[graphql(skip)]
    pub account_id: odf::AccountID,

    #[expect(dead_code)]
    #[graphql(skip)]
    pub data_room_dataset_id: odf::DatasetID,

    #[expect(dead_code)]
    #[graphql(skip)]
    pub announcements_dataset_id: odf::DatasetID,

    /// System time when this version was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this version was created/updated
    pub event_time: DateTime<Utc>,

    /// Symbolic name of the project
    pub ipnft_symbol: String,

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub ipnft_uid: String,

    /// Address of the IPNFT contract
    pub ipnft_address: String,

    // NOTE: For backward compatibility (and existing projects),
    //       we continue using BigInt type, which is wider than needed U256.
    /// Token ID withing the IPNFT contract
    pub ipnft_token_id: BigInt,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[ComplexObject]
impl MoleculeProjectV2 {
    /// Project's organizational account
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_account, skip_all)]
    async fn account(&self, _ctx: &Context<'_>) -> Result<Account> {
        todo!()
    }

    /// Strongly typed data room accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_data_room, skip_all)]
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<MoleculeDataRoomDatasetV2> {
        todo!()
    }

    /// Strongly typed announcements accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetV2> {
        todo!()
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeActivityFiltersV2>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        let _ = ctx;
        let _ = page;
        let _ = per_page;
        let _ = filters;
        // TODO: implement
        Ok(MoleculeActivityEventV2Connection::new(vec![], 0, 0))
    }
}

page_based_connection!(
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeProjectV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
