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
use internal_error::{ErrorIntoInternal, InternalError};

use crate::{
    MoleculeDataRoomActivityEntity,
    MoleculeGetDatasetError,
    MoleculeGlobalAnnouncementRecord,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewGlobalActivitiesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        filters: Option<MoleculeGlobalActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeGlobalActivitiesFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum MoleculeGlobalActivity {
    DataRoomActivity(MoleculeDataRoomActivityEntity),
    Announcement(MoleculeGlobalAnnouncementRecord),
}

impl MoleculeGlobalActivity {
    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::DataRoomActivity(entity) => entity.event_time,
            Self::Announcement(entity) => entity.system_columns.event_time,
        }
    }

    pub fn ipnft_uid(&self) -> &String {
        match self {
            Self::DataRoomActivity(entity) => &entity.ipnft_uid,
            Self::Announcement(entity) => &entity.record.ipnft_uid,
        }
    }
}

pub type MoleculeDataRoomActivityListing = EntityPageListing<MoleculeGlobalActivity>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewDataRoomActivitiesError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<MoleculeGetDatasetError> for MoleculeViewDataRoomActivitiesError {
    fn from(e: MoleculeGetDatasetError) -> Self {
        use MoleculeGetDatasetError as E;
        match e {
            E::Access(e) => e.into(),
            E::NotFound(_) | E::Internal(_) => e.int_err().into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
