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

use crate::{MoleculeAnnouncementsFilters, MoleculeGlobalAnnouncement};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewGlobalAnnouncementsUseCase: Send + Sync {
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        mode: MoleculeViewGlobalAnnouncementsMode,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalAnnouncementListing, MoleculeViewGlobalAnnouncementsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MoleculeViewGlobalAnnouncementsMode {
    LatestProjection,
    LatestSource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeGlobalAnnouncementListing = EntityPageListing<MoleculeGlobalAnnouncement>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewGlobalAnnouncementsError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
