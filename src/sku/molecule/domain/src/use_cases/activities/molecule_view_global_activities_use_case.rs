// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{EntityPageListing, PaginationOpts};
use internal_error::InternalError;

use crate::{MoleculeActivitiesFilters, MoleculeDataRoomActivity, MoleculeGlobalAnnouncement};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewGlobalActivitiesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeGlobalActivityListing = EntityPageListing<MoleculeGlobalActivity>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum MoleculeGlobalActivity {
    DataRoomActivity(MoleculeDataRoomActivity),
    Announcement(MoleculeGlobalAnnouncement),
}

impl MoleculeGlobalActivity {
    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::DataRoomActivity(entity) => entity.event_time,
            Self::Announcement(entity) => entity.announcement.event_time,
        }
    }

    pub fn ipnft_uid(&self) -> &String {
        match self {
            Self::DataRoomActivity(entity) => &entity.ipnft_uid,
            Self::Announcement(entity) => &entity.ipnft_uid,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewGlobalActivitiesError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
