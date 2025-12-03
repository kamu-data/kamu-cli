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

use crate::MoleculeProjectAnnouncementRecord;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeProjectAnnouncementEntriesService: Send + Sync {
    async fn get_project_announcement_entries(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeAnnouncementsCollectionReadError>;

    async fn find_project_announcement_entry_by_id(
        &self,
        project_announcements_dataset_id: &odf::DatasetID,
        as_of: Option<odf::Multihash>,
        announcement_id: uuid::Uuid,
    ) -> Result<Option<MoleculeProjectAnnouncementRecord>, MoleculeAnnouncementsCollectionReadError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeProjectAnnouncementListing = EntityPageListing<MoleculeProjectAnnouncementRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeAnnouncementsCollectionReadError {
    #[error(transparent)]
    AnnouncementsDatasetNotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
