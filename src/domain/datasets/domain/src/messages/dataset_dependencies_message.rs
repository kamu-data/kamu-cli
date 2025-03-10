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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_DEPENDENCIES_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetDependenciesMessage {
    pub dataset_id: odf::DatasetID,
    pub obsolete_upstream_ids: Vec<odf::DatasetID>,
    pub added_upstream_ids: Vec<odf::DatasetID>,
}

impl Message for DatasetDependenciesMessage {
    fn version() -> u32 {
        DATASET_DEPENDENCIES_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
