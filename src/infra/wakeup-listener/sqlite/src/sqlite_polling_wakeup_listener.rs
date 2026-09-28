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
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sqlite has no notifications, so this polls a cheap `SELECT MAX(id) FROM ...`
/// query with exponential backoff.
pub struct SqlitePollingWakeupListener {
    pool: Arc<sqlx::SqlitePool>,
    max_id_query: String,
    max_seen_id: Mutex<i64>,
}

impl SqlitePollingWakeupListener {
    pub fn new(pool: Arc<sqlx::SqlitePool>, max_id_query: impl Into<String>) -> Self {
        let max_id_query = max_id_query.into();

        Self {
            pool,
            max_id_query,
            max_seen_id: Mutex::new(0),
        }
    }

    async fn check_for_new_ids(&self) -> Result<Option<i64>, InternalError> {
        let max_present_id = {
            let (max_present_id,): (Option<i64>,) = sqlx::query_as(&self.max_id_query)
                .fetch_one(self.pool.as_ref())
                .await
                .unwrap_or((None,));

            Ok(max_present_id.unwrap_or_default())
        }?;

        let mut max_seen_id = self.max_seen_id.lock().unwrap();
        if max_present_id > *max_seen_id {
            *max_seen_id = max_present_id;
            Ok(Some(max_present_id))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WakeupListener for SqlitePollingWakeupListener {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll_interval = min_debounce_interval;

        loop {
            if let Some(_max_id) = self.check_for_new_ids().await? {
                return Ok(WakeHint::Signaled);
            }

            // Calculate remaining time
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // Timeout elapsed, no new work
                return Ok(WakeHint::Timeout);
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
