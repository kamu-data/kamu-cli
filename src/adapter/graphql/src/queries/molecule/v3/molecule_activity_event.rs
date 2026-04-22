// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::molecule::v3::{MoleculeAnnouncementEntry, MoleculeDataRoomEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum MoleculeActivityEvent {
    FileAdded(MoleculeActivityFileAdded),
    FileUpdated(MoleculeActivityFileUpdated),
    FileRemoved(MoleculeActivityFileRemoved),
    Announcement(MoleculeActivityAnnouncement),
}

impl MoleculeActivityEvent {
    pub fn file_added(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileAdded(MoleculeActivityFileAdded { entry })
    }

    pub fn file_updated(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileUpdated(MoleculeActivityFileUpdated { entry })
    }

    pub fn file_removed(entry: MoleculeDataRoomEntry) -> Self {
        Self::FileRemoved(MoleculeActivityFileRemoved { entry })
    }

    pub fn announcement(announcement: MoleculeAnnouncementEntry) -> Self {
        Self::Announcement(MoleculeActivityAnnouncement { announcement })
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
pub struct MoleculeActivityFileAdded {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileUpdated {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileRemoved {
    pub entry: MoleculeDataRoomEntry,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityAnnouncement {
    pub announcement: MoleculeAnnouncementEntry,
}

page_based_stream_connection!(
    MoleculeActivityEvent,
    MoleculeActivityEventConnection,
    MoleculeActivityEventEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
