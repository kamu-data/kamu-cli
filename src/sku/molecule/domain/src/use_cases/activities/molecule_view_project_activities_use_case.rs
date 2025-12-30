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

use crate::{
    MoleculeActivitiesFilters,
    MoleculeAnnouncement,
    MoleculeDataRoomActivity,
    MoleculeProject,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewProjectActivitiesUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewProjectActivitiesMode,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeViewProjectActivitiesMode {
    LatestProjection,
    LatestSource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeProjectActivityListing = EntityPageListing<MoleculeProjectActivity>;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum MoleculeProjectActivity {
    DataRoomActivity(MoleculeDataRoomActivity),
    Announcement(MoleculeAnnouncement),
}

impl MoleculeProjectActivity {
    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::DataRoomActivity(entity) => entity.event_time,
            Self::Announcement(entity) => entity.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
