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
/// Ends once a drain is idle: one that waited for the agent's drain is not,
/// since that drain's messages may still be undelivered.
///
/// The controller runtime drives the controllers of the catalog it is
/// registered in, so the catalog passed to `finalize` is the one that has both.
pub struct BackgroundWorkFinalizer {
    delivers_outbox: bool,
}

impl BackgroundWorkFinalizer {
    /// For a dispatching outbox, whose messages wait for its agent
    pub fn for_dispatching_outbox() -> Self {
        Self {
            delivers_outbox: true,
        }
    }

    /// For an immediate outbox, which delivered every message already
    pub fn for_immediate_outbox() -> Self {
        Self {
            delivers_outbox: false,
        }
    }

    pub async fn finalize(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        let outbox_agent = if self.delivers_outbox {
            Some(catalog.get_one::<dyn OutboxAgent>().int_err()?)
        } else {
            None
        };
        let mut controller_drains = catalog
            .get_one::<dyn ResourceControllerRuntime>()
            .int_err()?
            .new_drain_session()?;

        loop {
            if let Some(outbox_agent) = &outbox_agent {
                outbox_agent.run_while_has_tasks().await?;
            }

            if controller_drains.drain_due_resources().await?.is_idle() {
                return Ok(());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
