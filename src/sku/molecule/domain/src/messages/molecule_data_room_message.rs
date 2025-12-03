// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_datasets::CollectionPath;
use messaging_outbox::Message;

use crate::MoleculeDataRoomEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_DATA_ROOM_MESSAGE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of a Molecule project
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MoleculeDataRoomMessage {
    EntryCreated(MoleculeDataRoomMessageEntryCreated),
    EntryUpdated(MoleculeDataRoomMessageEntryUpdated),
    EntryMoved(MoleculeDataRoomMessageEntryMoved),
    EntryRemoved(MoleculeDataRoomMessageEntryRemoved),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeDataRoomMessage {
    pub fn created(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ipnft_uid: String,
        data_room_entry: MoleculeDataRoomEntry,
    ) -> Self {
        Self::EntryCreated(MoleculeDataRoomMessageEntryCreated {
            event_time,
            molecule_account_id,
            project_account_id,
            ipnft_uid,
            data_room_entry,
        })
    }

    pub fn updated(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ipnft_uid: String,
        data_room_entry: MoleculeDataRoomEntry,
    ) -> Self {
        Self::EntryUpdated(MoleculeDataRoomMessageEntryUpdated {
            event_time,
            molecule_account_id,
            project_account_id,
            ipnft_uid,
            data_room_entry,
        })
    }

    pub fn moved(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ipnft_uid: String,
        path_from: CollectionPath,
        path_to: CollectionPath,
    ) -> Self {
        Self::EntryMoved(MoleculeDataRoomMessageEntryMoved {
            event_time,
            molecule_account_id,
            project_account_id,
            ipnft_uid,
            path_from,
            path_to,
        })
    }

    pub fn removed(
        event_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ipnft_uid: String,
        path: CollectionPath,
    ) -> Self {
        Self::EntryRemoved(MoleculeDataRoomMessageEntryRemoved {
            event_time,
            molecule_account_id,
            project_account_id,
            ipnft_uid,
            path,
        })
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            MoleculeDataRoomMessage::EntryCreated(msg) => msg.event_time,
            MoleculeDataRoomMessage::EntryUpdated(msg) => msg.event_time,
            MoleculeDataRoomMessage::EntryMoved(msg) => msg.event_time,
            MoleculeDataRoomMessage::EntryRemoved(msg) => msg.event_time,
        }
    }

    pub fn molecule_account_id(&self) -> &odf::AccountID {
        match self {
            MoleculeDataRoomMessage::EntryCreated(msg) => &msg.molecule_account_id,
            MoleculeDataRoomMessage::EntryUpdated(msg) => &msg.molecule_account_id,
            MoleculeDataRoomMessage::EntryMoved(msg) => &msg.molecule_account_id,
            MoleculeDataRoomMessage::EntryRemoved(msg) => &msg.molecule_account_id,
        }
    }

    pub fn project_account_id(&self) -> &odf::AccountID {
        match self {
            MoleculeDataRoomMessage::EntryCreated(msg) => &msg.project_account_id,
            MoleculeDataRoomMessage::EntryUpdated(msg) => &msg.project_account_id,
            MoleculeDataRoomMessage::EntryMoved(msg) => &msg.project_account_id,
            MoleculeDataRoomMessage::EntryRemoved(msg) => &msg.project_account_id,
        }
    }

    pub fn ipnft_uid(&self) -> &String {
        match self {
            MoleculeDataRoomMessage::EntryCreated(msg) => &msg.ipnft_uid,
            MoleculeDataRoomMessage::EntryUpdated(msg) => &msg.ipnft_uid,
            MoleculeDataRoomMessage::EntryMoved(msg) => &msg.ipnft_uid,
            MoleculeDataRoomMessage::EntryRemoved(msg) => &msg.ipnft_uid,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for MoleculeDataRoomMessage {
    fn version() -> u32 {
        MOLECULE_DATA_ROOM_MESSAGE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomMessageEntryCreated {
    event_time: DateTime<Utc>,
    molecule_account_id: odf::AccountID,
    project_account_id: odf::AccountID,
    ipnft_uid: String,
    data_room_entry: MoleculeDataRoomEntry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomMessageEntryUpdated {
    event_time: DateTime<Utc>,
    molecule_account_id: odf::AccountID,
    project_account_id: odf::AccountID,
    ipnft_uid: String,
    data_room_entry: MoleculeDataRoomEntry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomMessageEntryMoved {
    event_time: DateTime<Utc>,
    molecule_account_id: odf::AccountID,
    project_account_id: odf::AccountID,
    ipnft_uid: String,
    path_from: CollectionPath,
    path_to: CollectionPath,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomMessageEntryRemoved {
    event_time: DateTime<Utc>,
    molecule_account_id: odf::AccountID,
    project_account_id: odf::AccountID,
    ipnft_uid: String,
    path: CollectionPath,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
