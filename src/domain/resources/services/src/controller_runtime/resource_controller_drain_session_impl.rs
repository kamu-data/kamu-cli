// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::controller_runtime::{ResourceControllerDrainHoldBack, ResourceControllerLoop};
use crate::domain::{ResourceControllerDrainSession, ResourceControllerSessionDrainReport};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A loop per registered controller, each with a hold-back kept for the whole
/// session
pub(crate) struct ResourceControllerDrainSessionImpl {
    loops: Vec<(ResourceControllerLoop, ResourceControllerDrainHoldBack)>,
}

impl ResourceControllerDrainSessionImpl {
    pub fn new(loops: Vec<ResourceControllerLoop>) -> Self {
        Self {
            loops: loops
                .into_iter()
                .map(|controller_loop| {
                    (controller_loop, ResourceControllerDrainHoldBack::default())
                })
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl ResourceControllerDrainSession for ResourceControllerDrainSessionImpl {
    async fn drain_due_resources(
        &mut self,
    ) -> Result<ResourceControllerSessionDrainReport, InternalError> {
        let mut report = ResourceControllerSessionDrainReport {
            processed: 0,
            waited_for_another_drain: false,
        };
        for (controller_loop, hold_back) in &mut self.loops {
            let drained = controller_loop.drain(None, hold_back).await?;
            report.processed += drained.processed();
            report.waited_for_another_drain |= drained.waited_for_another_drain();
        }
        Ok(report)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
