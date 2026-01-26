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

use crate::DatasetProperties;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const REBAC_DATASET_PROPERTIES_MESSAGE_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the lifecycle of an access token
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebacDatasetPropertiesMessage {
    /// Message indicating dataset `ReBAC` properties
    ///  have been created or updated
    Modified(RebacDatasetPropertiesMessageModified),

    /// Message indicating dataset `ReBAC` properties
    ///  have been deleted
    Deleted(RebacDatasetPropertiesMessageDeleted),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RebacDatasetPropertiesMessage {
    pub fn modified(dataset_id: odf::DatasetID, dataset_properties: DatasetProperties) -> Self {
        Self::Modified(RebacDatasetPropertiesMessageModified {
            dataset_id,
            dataset_properties,
        })
    }

    pub fn deleted(dataset_id: odf::DatasetID) -> Self {
        Self::Deleted(RebacDatasetPropertiesMessageDeleted { dataset_id })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for RebacDatasetPropertiesMessage {
    fn version() -> u32 {
        REBAC_DATASET_PROPERTIES_MESSAGE_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a modified dataset `ReBAC` properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebacDatasetPropertiesMessageModified {
    pub dataset_id: odf::DatasetID,
    pub dataset_properties: DatasetProperties,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a deleted dataset `ReBAC` properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebacDatasetPropertiesMessageDeleted {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
