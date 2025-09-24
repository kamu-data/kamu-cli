// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::Catalog;
use internal_error::InternalError;
use kamu_task_system::TaskID;

use crate::{FlowAgentLoopSynchronizer, FlowID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowAgentTestDriver: Sync + Send {
    /// Pretends it is time to schedule the given flow that was in Queued state
    async fn mimic_flow_scheduled(
        &self,
        target_catalog: &Catalog,
        flow_id: FlowID,
        schedule_time: DateTime<Utc>,
    ) -> Result<TaskID, InternalError>;

    /// Set loop synchronizer to be used by the agent
    async fn set_loop_synchronizer(
        &self,
        synchronizer: Arc<dyn FlowAgentLoopSynchronizer>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
