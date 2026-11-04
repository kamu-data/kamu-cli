// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_molecule_domain::{MoleculeDataRoomActivity, MoleculeDataRoomFileActivityType};

use crate::prelude::*;
use crate::queries::molecule::v3::{MoleculeDataRoomEntry, MoleculeProject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Activity event of a versioned file in the data room
#[derive(Union)]
pub enum MoleculeVersionedFileActivityEvent {
    /// File was added to the data room
    Added(MoleculeActivityFileAdded),
    /// New version of the file was uploaded, or the file was moved or its
    /// metadata changed
    Updated(MoleculeActivityFileUpdated),
    /// File was removed from the data room
    Removed(MoleculeActivityFileRemoved),
}

impl MoleculeVersionedFileActivityEvent {
    pub fn from_data_room_activity(
        project: &Arc<MoleculeProject>,
        activity: MoleculeDataRoomActivity,
    ) -> Self {
        let activity_type = activity.activity_type;
        let entry = MoleculeDataRoomEntry::new_from_data_room_activity_entity(project, activity);

        match activity_type {
            MoleculeDataRoomFileActivityType::Added => {
                Self::Added(MoleculeActivityFileAdded { entry })
            }
            MoleculeDataRoomFileActivityType::Updated => {
                Self::Updated(MoleculeActivityFileUpdated { entry })
            }
            MoleculeDataRoomFileActivityType::Removed => {
                Self::Removed(MoleculeActivityFileRemoved { entry })
            }
        }
    }
}

/// File was added to the data room
#[derive(SimpleObject)]
pub struct MoleculeActivityFileAdded {
    /// Data room entry as recorded by this event
    pub entry: MoleculeDataRoomEntry,
}

/// New version of a file was uploaded, or a file was moved or its metadata
/// changed
#[derive(SimpleObject)]
pub struct MoleculeActivityFileUpdated {
    /// Data room entry as recorded by this event
    pub entry: MoleculeDataRoomEntry,
}

/// File was removed from the data room
#[derive(SimpleObject)]
pub struct MoleculeActivityFileRemoved {
    /// Data room entry as recorded by this event
    pub entry: MoleculeDataRoomEntry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
