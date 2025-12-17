// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use database_common::{EntityPageListing, PaginationOpts};
use internal_error::InternalError;

use crate::{MoleculeDataRoomActivity, MoleculeGlobalAnnouncement};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeSearchUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeSearchFilters {
    pub by_ipnft_uids: Option<Vec<String>>,
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
    pub by_types: Option<Vec<MoleculeSearchType>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MoleculeSearchType {
    DataRoomActivity,
    Announcement,
}

impl MoleculeSearchType {
    pub fn default_types() -> HashSet<MoleculeSearchType> {
        [Self::DataRoomActivity, Self::Announcement].into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeSearchHitsListing = EntityPageListing<MoleculeSearchHit>;

#[derive(Debug)]
pub enum MoleculeSearchHit {
    DataRoomActivity(MoleculeDataRoomActivity),
    Announcement(MoleculeGlobalAnnouncement),
}

impl MoleculeSearchHit {
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
pub enum MoleculeSearchError {
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
