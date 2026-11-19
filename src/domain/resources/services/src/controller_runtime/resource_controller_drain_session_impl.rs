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
    loops: Vec<SessionLoop>,
}

struct SessionLoop {
    controller_loop: ResourceControllerLoop,
    hold_back: ResourceControllerDrainHoldBack,
    /// `process` calls of every drain of the controller, as of the session's
    /// start or its previous drain
    processed_by_all_drains: usize,
}

impl ResourceControllerDrainSessionImpl {
    pub fn new(loops: Vec<ResourceControllerLoop>) -> Self {
        Self {
            loops: loops
                .into_iter()
                .map(|controller_loop| SessionLoop {
                    processed_by_all_drains: controller_loop.processed_by_all_drains(),
                    controller_loop,
                    hold_back: ResourceControllerDrainHoldBack::default(),
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
            processed_by_other_drains: 0,
        };
        for session_loop in &mut self.loops {
            let drained = session_loop
                .controller_loop
                .drain(None, &mut session_loop.hold_back)
                .await?;
            let processed_by_all_drains = session_loop.controller_loop.processed_by_all_drains();

            report.processed += drained.processed();
            report.processed_by_other_drains += processed_by_all_drains
                - session_loop.processed_by_all_drains
                - drained.processed();
            session_loop.processed_by_all_drains = processed_by_all_drains;
        }
        Ok(report)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
