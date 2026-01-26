// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::AuthorizedAccount;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const REBAC_DATASET_ACCOUNT_RELATIONS_MESSAGE_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of an access token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebacDatasetRelationsMessage {
    /// Message indicating dataset `ReBAC` relations with accounts
    ///   have been created or updated
    Modified(RebacDatasetRelationsMessageModified),

    /// Message indicating dataset `ReBAC` relations with accounts
    ///  have been deleted
    Deleted(RebacDatasetRelationsMessageDeleted),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RebacDatasetRelationsMessage {
    pub fn modified(
        dataset_id: odf::DatasetID,
        authorized_accounts: Vec<AuthorizedAccount>,
    ) -> Self {
        Self::Modified(RebacDatasetRelationsMessageModified {
            dataset_id,
            authorized_accounts,
        })
    }

    pub fn deleted(dataset_id: odf::DatasetID) -> Self {
        Self::Deleted(RebacDatasetRelationsMessageDeleted { dataset_id })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for RebacDatasetRelationsMessage {
    fn version() -> u32 {
        REBAC_DATASET_ACCOUNT_RELATIONS_MESSAGE_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a modified dataset `ReBAC` properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebacDatasetRelationsMessageModified {
    pub dataset_id: odf::DatasetID,
    pub authorized_accounts: Vec<AuthorizedAccount>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a deleted dataset `ReBAC` properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebacDatasetRelationsMessageDeleted {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
