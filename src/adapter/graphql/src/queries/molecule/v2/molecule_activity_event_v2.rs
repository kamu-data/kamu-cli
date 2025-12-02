// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::molecule::v2::{MoleculeAnnouncementEntry, MoleculeDataRoomEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum MoleculeActivityEventV2 {
    FileAdded(MoleculeActivityFileAddedV2),
    FileUpdated(MoleculeActivityFileUpdatedV2),
    FileRemoved(MoleculeActivityFileRemovedV2),
    Announcement(MoleculeActivityAnnouncementV2),
}

impl MoleculeActivityEventV2 {
    pub fn file_added(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileAdded(MoleculeActivityFileAddedV2 { entry })
    }

    pub fn file_updated(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileUpdated(MoleculeActivityFileUpdatedV2 { entry })
    }

    pub fn file_removed(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileRemoved(MoleculeActivityFileRemovedV2 { entry })
    }

    #[expect(dead_code)]
    pub fn announcement(announcement: MoleculeAnnouncementEntry) -> Self {
        Self::Announcement(MoleculeActivityAnnouncementV2 { announcement })
    }

    #[expect(dead_code)]
    pub fn data_room_entry(&self) -> Option<&MoleculeDataRoomEntry> {
        match self {
            Self::FileAdded(event) => Some(&event.entry),
            Self::FileUpdated(event) => Some(&event.entry),
            Self::FileRemoved(event) => Some(&event.entry),
            Self::Announcement(_) => None,
        }
    }
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileAddedV2 {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileUpdatedV2 {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileRemovedV2 {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityAnnouncementV2 {
    pub announcement: MoleculeAnnouncementEntry,
}

page_based_stream_connection!(
    MoleculeActivityEventV2,
    MoleculeActivityEventV2Connection,
    MoleculeActivityEventV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
