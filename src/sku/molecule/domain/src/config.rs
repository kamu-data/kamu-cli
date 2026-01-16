// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    MoleculeSearchMode,
    MoleculeViewDataRoomEntriesMode,
    MoleculeViewGlobalActivitiesMode,
    MoleculeViewGlobalAnnouncementsMode,
    MoleculeViewProjectActivitiesMode,
    MoleculeViewProjectAnnouncementsMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct MoleculeConfig {
    pub enable_reads_from_projections: bool,
}

impl Default for MoleculeConfig {
    fn default() -> Self {
        Self {
            enable_reads_from_projections: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeConfig {
    pub fn view_data_room_entries_mode(&self) -> MoleculeViewDataRoomEntriesMode {
        if self.enable_reads_from_projections {
            MoleculeViewDataRoomEntriesMode::LatestProjection
        } else {
            MoleculeViewDataRoomEntriesMode::LatestSource
        }
    }

    pub fn view_global_activities_mode(&self) -> MoleculeViewGlobalActivitiesMode {
        if self.enable_reads_from_projections {
            MoleculeViewGlobalActivitiesMode::LatestProjection
        } else {
            MoleculeViewGlobalActivitiesMode::LatestSource
        }
    }

    pub fn view_project_activities_mode(&self) -> MoleculeViewProjectActivitiesMode {
        if self.enable_reads_from_projections {
            MoleculeViewProjectActivitiesMode::LatestProjection
        } else {
            MoleculeViewProjectActivitiesMode::LatestSource
        }
    }

    pub fn view_global_announcements_mode(&self) -> MoleculeViewGlobalAnnouncementsMode {
        if self.enable_reads_from_projections {
            MoleculeViewGlobalAnnouncementsMode::LatestProjection
        } else {
            MoleculeViewGlobalAnnouncementsMode::LatestSource
        }
    }

    pub fn view_project_announcements_mode(&self) -> MoleculeViewProjectAnnouncementsMode {
        if self.enable_reads_from_projections {
            MoleculeViewProjectAnnouncementsMode::LatestProjection
        } else {
            MoleculeViewProjectAnnouncementsMode::LatestSource
        }
    }

    pub fn search_mode(&self) -> MoleculeSearchMode {
        if self.enable_reads_from_projections {
            MoleculeSearchMode::ViaSearchIndex
        } else {
            MoleculeSearchMode::ViaChangelog
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
