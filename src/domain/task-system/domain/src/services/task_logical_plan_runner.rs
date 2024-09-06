// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::InternalError;

use crate::{LogicalPlan, TaskOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskLogicalPlanRunner: Send + Sync {
    async fn run_plan(&self, logical_plan: &LogicalPlan) -> Result<TaskOutcome, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
