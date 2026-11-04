// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::molecule::v3::MoleculeVersionedFileActivityEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Activity event of a single data room entry
#[derive(Union)]
pub enum MoleculeDataRoomEntryActivityEvent {
    /// Activity of the versioned file the entry links
    #[graphql(flatten)]
    VersionedFile(MoleculeVersionedFileActivityEvent),
}

page_based_stream_connection!(
    MoleculeDataRoomEntryActivityEvent,
    MoleculeDataRoomEntryActivityEventConnection,
    MoleculeDataRoomEntryActivityEventEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
