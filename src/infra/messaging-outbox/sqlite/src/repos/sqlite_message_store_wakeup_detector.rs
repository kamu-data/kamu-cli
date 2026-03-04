// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use internal_error::InternalError;
use messaging_outbox::{MessageStoreWakeHint, MessageStoreWakeupDetector};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteMessageStoreWakeupDetector {
    pool: Arc<sqlx::SqlitePool>,
    message_max_fetch_query: String,
    max_seen_event_id: Mutex<i64>,
}

impl SqliteMessageStoreWakeupDetector {
    pub fn new(pool: Arc<sqlx::SqlitePool>, message_max_fetch_query: impl Into<String>) -> Self {
        let message_max_fetch_query = message_max_fetch_query.into();

        Self {
            pool,
            message_max_fetch_query,
            max_seen_event_id: Mutex::new(0),
        }
    }

    async fn check_for_new_events(&self) -> Result<Option<i64>, InternalError> {
        let max_present_event_id = {
            let (max_present_event_id,): (Option<i64>,) =
                sqlx::query_as(&self.message_max_fetch_query)
                    .fetch_one(self.pool.as_ref())
                    .await
                    .unwrap_or((None,));

            Ok(max_present_event_id.unwrap_or_default())
        }?;

        let mut max_seen_event_id = self.max_seen_event_id.lock().unwrap();
        if max_present_event_id > *max_seen_event_id {
            *max_seen_event_id = max_present_event_id;
            Ok(Some(max_present_event_id))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageStoreWakeupDetector for SqliteMessageStoreWakeupDetector {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<MessageStoreWakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll_interval = min_debounce_interval;

        loop {
            // Check for new events
            if let Some(_max_event_id) = self.check_for_new_events().await? {
                return Ok(MessageStoreWakeHint::NewMessages);
            }

            // Calculate remaining time
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Timeout elapsed, no new work
                return Ok(MessageStoreWakeHint::Timeout);
            }

            // Sleep for the shorter of poll_interval or remaining time
            let sleep_duration = std::cmp::min(poll_interval, remaining);
            tokio::time::sleep(sleep_duration).await;

            // Increase poll interval for next iteration
            // (exponential backoff with max limit of timeout)
            poll_interval = std::cmp::min(poll_interval * 2, timeout);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
