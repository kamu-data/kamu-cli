// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_http::e2e::E2EMutationFinalizer;
use kamu_resources::ResourceControllerRuntime;
use messaging_outbox::OutboxAgent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes the background work a command, or a mutating request of the e2e
/// API server, left behind: delivers the Outbox messages, alternating with
/// drains of the resource controllers, since a message may make a resource
/// actionable, and processing it posts messages of its own. Ends once a drain
/// processes nothing.
pub(crate) struct BackgroundWorkFinalizer;

impl BackgroundWorkFinalizer {
    pub async fn finalize(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().int_err()?;
        let mut controller_drains = catalog
            .get_one::<dyn ResourceControllerRuntime>()
            .int_err()?
            .new_drain_session()?;

        loop {
            outbox_agent.run_while_has_tasks().await?;

            if controller_drains.drain_due_resources().await? == 0 {
                return Ok(());
            }
        }
    }
}

#[async_trait::async_trait]
impl E2EMutationFinalizer for BackgroundWorkFinalizer {
    async fn finalize(&self, base_catalog: &dill::Catalog) -> Result<(), InternalError> {
        Self::finalize(self, base_catalog).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
