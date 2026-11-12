// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use messaging_outbox::OutboxAgent;

use crate::domain::ResourceControllerRuntime;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes the background work a command, a mutating request of the e2e API
/// server, or a test write left behind: delivers the Outbox messages,
/// alternating with drains of the resource controllers, since a message may
/// make a resource actionable, and processing it posts messages of its own.
/// Ends once a drain processes nothing.
///
/// The controller runtime drives the controllers of the catalog it is
/// registered in, so `catalog` is the one that has both. A catalog without an
/// outbox agent has an immediate outbox, which delivered every message
/// already.
pub struct BackgroundWorkFinalizer;

impl BackgroundWorkFinalizer {
    pub async fn finalize(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().ok();
        let mut controller_drains = catalog
            .get_one::<dyn ResourceControllerRuntime>()
            .int_err()?
            .new_drain_session()?;

        loop {
            if let Some(outbox_agent) = &outbox_agent {
                outbox_agent.run_while_has_tasks().await?;
            }

            if controller_drains.drain_due_resources().await? == 0 {
                return Ok(());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
