// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, RwLock};

use dill::*;
use internal_error::InternalError;

use crate::{Outbox, OutboxImmediateImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PausableImmediateOutboxImpl {
    immediate_outbox: Arc<OutboxImmediateImpl>,
    paused: Arc<RwLock<bool>>,
}

#[component(pub)]
#[scope(Singleton)]
impl PausableImmediateOutboxImpl {
    pub fn new(immediate_outbox: Arc<OutboxImmediateImpl>) -> Self {
        Self {
            immediate_outbox,
            paused: Arc::new(RwLock::new(false)),
        }
    }

    pub fn pause(&self) {
        let mut paused = self.paused.write().unwrap();
        *paused = true;
    }

    pub fn resume(&self) {
        let mut paused = self.paused.write().unwrap();
        *paused = false;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Outbox for PausableImmediateOutboxImpl {
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
        version: u32,
    ) -> Result<(), InternalError> {
        let paused = *self.paused.read().unwrap();
        if paused {
            Ok(())
        } else {
            self.immediate_outbox
                .post_message_as_json(producer_name, content_json, version)
                .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
