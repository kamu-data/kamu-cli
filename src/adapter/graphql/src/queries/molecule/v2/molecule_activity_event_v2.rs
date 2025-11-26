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
