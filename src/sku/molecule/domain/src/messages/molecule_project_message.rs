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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MOLECULE_PROJECT_MESSAGE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of a Molecule project
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MoleculeProjectMessage {
    /// Message indicating that a project has been created.
    Created(MoleculeProjectMessageCreated),

    /// Message indicating that a project has been disabled.
    Disabled(MoleculeProjectMessageDisabled),

    /// Message indicating that a project has been re-enabled.
    Reenabled(MoleculeProjectMessageReenabled),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeProjectMessage {
    pub fn created(
        event_time: DateTime<Utc>,
        system_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ocl_id: String,
        symbol: String,
    ) -> Self {
        Self::Created(MoleculeProjectMessageCreated {
            event_time,
            system_time,
            molecule_account_id,
            project_account_id,
            ocl_id,
            symbol,
        })
    }

    pub fn disabled(
        event_time: DateTime<Utc>,
        system_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ocl_id: String,
        symbol: String,
    ) -> Self {
        Self::Disabled(MoleculeProjectMessageDisabled {
            event_time,
            system_time,
            molecule_account_id,
            project_account_id,
            ocl_id,
            symbol,
        })
    }

    pub fn reenabled(
        event_time: DateTime<Utc>,
        system_time: DateTime<Utc>,
        molecule_account_id: odf::AccountID,
        project_account_id: odf::AccountID,
        ocl_id: String,
        symbol: String,
    ) -> Self {
        Self::Reenabled(MoleculeProjectMessageReenabled {
            event_time,
            system_time,
            molecule_account_id,
            project_account_id,
            ocl_id,
            symbol,
        })
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            MoleculeProjectMessage::Created(msg) => msg.event_time,
            MoleculeProjectMessage::Disabled(msg) => msg.event_time,
            MoleculeProjectMessage::Reenabled(msg) => msg.event_time,
        }
    }

    pub fn molecule_account_id(&self) -> &odf::AccountID {
        match self {
            MoleculeProjectMessage::Created(msg) => &msg.molecule_account_id,
            MoleculeProjectMessage::Disabled(msg) => &msg.molecule_account_id,
            MoleculeProjectMessage::Reenabled(msg) => &msg.molecule_account_id,
        }
    }

    pub fn project_account_id(&self) -> &odf::AccountID {
        match self {
            MoleculeProjectMessage::Created(msg) => &msg.project_account_id,
            MoleculeProjectMessage::Disabled(msg) => &msg.project_account_id,
            MoleculeProjectMessage::Reenabled(msg) => &msg.project_account_id,
        }
    }

    pub fn ocl_id(&self) -> &str {
        match self {
            MoleculeProjectMessage::Created(msg) => &msg.ocl_id,
            MoleculeProjectMessage::Disabled(msg) => &msg.ocl_id,
            MoleculeProjectMessage::Reenabled(msg) => &msg.ocl_id,
        }
    }
}

impl Message for MoleculeProjectMessage {
    fn version() -> u32 {
        MOLECULE_PROJECT_MESSAGE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a newly created Molecule project.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectMessageCreated {
    pub event_time: DateTime<Utc>,
    pub system_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub project_account_id: odf::AccountID,
    pub ocl_id: String,
    pub symbol: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a disabled Molecule project.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectMessageDisabled {
    pub event_time: DateTime<Utc>,
    pub system_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub project_account_id: odf::AccountID,
    pub ocl_id: String,
    pub symbol: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a re-enabled Molecule project.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectMessageReenabled {
    pub event_time: DateTime<Utc>,
    pub system_time: DateTime<Utc>,
    pub molecule_account_id: odf::AccountID,
    pub project_account_id: odf::AccountID,
    pub ocl_id: String,
    pub symbol: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
