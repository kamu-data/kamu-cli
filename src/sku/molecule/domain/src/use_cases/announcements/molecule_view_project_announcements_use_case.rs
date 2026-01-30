// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityPageListing, PaginationOpts};
use internal_error::InternalError;
use kamu_accounts::LoggedAccount;

use crate::{MoleculeAnnouncement, MoleculeAnnouncementsFilters, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewProjectAnnouncementsUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewProjectAnnouncementsMode,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeViewProjectAnnouncementsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeViewProjectAnnouncementsMode {
    LatestProjection,
    LatestSource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeProjectAnnouncementListing = EntityPageListing<MoleculeAnnouncement>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewProjectAnnouncementsError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
