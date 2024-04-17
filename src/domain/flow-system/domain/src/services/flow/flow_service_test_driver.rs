// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_task_system::TaskID;

use crate::FlowID;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowServiceTestDriver: Sync + Send {
    /// Pretends running started
    fn mimic_running_started(&self);

    /// Pretends it is time to schedule the given flow that was in Queued state
    async fn mimic_flow_scheduled(
        &self,
        flow_id: FlowID,
        schedule_time: DateTime<Utc>,
    ) -> Result<TaskID, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
