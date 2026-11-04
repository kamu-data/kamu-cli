// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::molecule::v3::{
    MoleculeAnnouncementEntry,
    MoleculeDataRoomEntry,
    MoleculeVersionedFileActivityEvent,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Activity event in the data room or announcements of a project
#[derive(Union)]
pub enum MoleculeActivityEvent {
    /// Activity of a versioned file
    #[graphql(flatten)]
    VersionedFile(MoleculeVersionedFileActivityEvent),
    /// Announcement was published
    Announcement(MoleculeActivityAnnouncement),
}

impl MoleculeActivityEvent {
    pub fn announcement(announcement: MoleculeAnnouncementEntry) -> Self {
        Self::Announcement(MoleculeActivityAnnouncement { announcement })
    }

    #[expect(dead_code, reason = "API completeness")]
    pub fn data_room_entry(&self) -> Option<&MoleculeDataRoomEntry> {
        match self {
            Self::VersionedFile(MoleculeVersionedFileActivityEvent::Added(event)) => {
                Some(&event.entry)
            }
            Self::VersionedFile(MoleculeVersionedFileActivityEvent::Updated(event)) => {
                Some(&event.entry)
            }
            Self::VersionedFile(MoleculeVersionedFileActivityEvent::Removed(event)) => {
                Some(&event.entry)
            }
            Self::Announcement(_) => None,
        }
    }
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
