// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::Message;

use crate::MoleculeDataRoomActivityPayloadRecord;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_ACTIVITY_MESSAGE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the announcements published in Molecule
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MoleculeActivityMessage {
    Published(MoleculeActivityMessagePublished),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeActivityMessage {
    pub fn published(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        activity_record: MoleculeDataRoomActivityPayloadRecord,
    ) -> Self {
        Self::Published(MoleculeActivityMessagePublished {
            event_time,
            molecule_account_id,
            activity_record,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for MoleculeActivityMessage {
    fn version() -> u32 {
        MOLECULE_ACTIVITY_MESSAGE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeActivityMessagePublished {
    pub event_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub activity_record: MoleculeDataRoomActivityPayloadRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
