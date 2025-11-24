// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncements,
    MoleculeDataRoom,
};
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct MoleculeProjectV2 {
    #[graphql(skip)]
    pub account_id: odf::AccountID,

    #[graphql(skip)]
    pub data_room_dataset_id: odf::DatasetID,

    #[expect(dead_code)]
    #[graphql(skip)]
    pub announcements_dataset_id: odf::DatasetID,

    /// System time when this project was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this project was created/updated
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
    async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.account_id.clone()).await?;
        Ok(account)
    }

    /// Strongly typed data room accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_data_room, skip_all)]
    async fn data_room(&self, ctx: &Context<'_>) -> Result<MoleculeDataRoom> {
        let Some(dataset) =
            Dataset::try_from_ref(ctx, &self.data_room_dataset_id.as_local_ref()).await?
        else {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Dataset inaccessible".into(),
            )));
        };

        Ok(MoleculeDataRoom {
            dataset,
            project: Arc::new(self.clone()),
        })
    }

    /// Strongly typed announcements accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncements> {
        todo!()
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeProjectActivityFilters>,
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

#[derive(InputObject)]
pub struct MoleculeProjectActivityFilters {
    // TODO: replace w/ real filters.
    /// This filter is provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serde
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeProjectV2 {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let record: MoleculeProjectV2ChangelogRecord = serde_json::from_value(record).int_err()?;

        Ok(Self {
            system_time: record.system_time,
            event_time: record.event_time,
            ipnft_symbol: record.data.ipnft_symbol,
            ipnft_uid: record.data.ipnft_uid,
            ipnft_address: record.data.ipnft_address,
            ipnft_token_id: BigInt::new(record.data.ipnft_token_id.parse().int_err()?),
            account_id: record.data.account_id,
            data_room_dataset_id: record.data.data_room_dataset_id,
            announcements_dataset_id: record.data.announcements_dataset_id,
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectV2ChangelogRecord {
    pub offset: u64,

    pub op: u8,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,

    #[serde(flatten)]
    pub data: MoleculeProjectV2Data,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectV2Data {
    pub ipnft_symbol: String,

    pub ipnft_uid: String,

    pub ipnft_address: String,

    pub ipnft_token_id: String,

    pub account_id: odf::AccountID,

    pub data_room_dataset_id: odf::DatasetID,

    pub announcements_dataset_id: odf::DatasetID,
}
