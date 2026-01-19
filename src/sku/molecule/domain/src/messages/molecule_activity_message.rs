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
    WriteRequested(MoleculeActivityMessageWriteRequested),
    Published(MoleculeActivityMessagePublished),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeActivityMessage {
    pub fn write_requested(
        molecule_subject_account_name: odf::AccountName,
        source_event_time: Option<DateTime<Utc>>,
        activity_record: MoleculeDataRoomActivityPayloadRecord,
    ) -> Self {
        Self::WriteRequested(MoleculeActivityMessageWriteRequested {
            molecule_subject_account_name,
            source_event_time,
            activity_record,
        })
    }

    pub fn published(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        offset: u64,
        activity_record: MoleculeDataRoomActivityPayloadRecord,
    ) -> Self {
        Self::Published(MoleculeActivityMessagePublished {
            event_time,
            molecule_account_id,
            offset,
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
pub struct MoleculeActivityMessageWriteRequested {
    pub molecule_subject_account_name: odf::AccountName,
    pub source_event_time: Option<DateTime<Utc>>,
    pub activity_record: MoleculeDataRoomActivityPayloadRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeActivityMessagePublished {
    pub event_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub offset: u64,
    pub activity_record: MoleculeDataRoomActivityPayloadRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
