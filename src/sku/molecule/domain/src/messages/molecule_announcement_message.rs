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

use crate::MoleculeAnnouncementPayloadRecord;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_ANNOUNCEMENT_MESSAGE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the announcements published in Molecule
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MoleculeAnnouncementMessage {
    Published(MoleculeAnnouncementMessagePublished),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeAnnouncementMessage {
    pub fn published(
        event_time: DateTime<Utc>,
        system_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        ipnft_uid: String,
        announcement_record: MoleculeAnnouncementPayloadRecord,
    ) -> Self {
        Self::Published(MoleculeAnnouncementMessagePublished {
            event_time,
            system_time,
            molecule_account_id,
            ipnft_uid,
            announcement_record,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for MoleculeAnnouncementMessage {
    fn version() -> u32 {
        MOLECULE_ANNOUNCEMENT_MESSAGE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeAnnouncementMessagePublished {
    pub event_time: DateTime<Utc>,
    pub system_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub ipnft_uid: String,
    pub announcement_record: MoleculeAnnouncementPayloadRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
