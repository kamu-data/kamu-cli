// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::TaskOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task that can be used for testing the scheduling system
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogicalPlanProbe {
    /// ID of the dataset this task should be associated with
    pub dataset_id: Option<odf::DatasetID>,
    pub busy_time: Option<std::time::Duration>,
    pub end_with_outcome: Option<TaskOutcome>,
}

impl LogicalPlanProbe {
    pub const SERIALIZATION_TYPE_ID: &str = "Probe";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
