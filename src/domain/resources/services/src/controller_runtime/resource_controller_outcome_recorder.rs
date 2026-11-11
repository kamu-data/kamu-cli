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
use database_common::DatabaseTransactionRunner;
use internal_error::InternalError;
use retry_backoff::RetryBackoff;

use crate::controller_runtime::{ResourceControllerRegistration, ResourceProcessAttempt};
use crate::domain::{ResourceControllerStateRepository, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Writes the bookkeeping row of a finished attempt.
///
/// It runs in a transaction of its own, after `process` committed its own
/// work. A crash between the two loses only the record: the resource stays
/// eligible and `process`, being idempotent, is asked again.
pub(crate) struct ResourceControllerOutcomeRecorder<'a> {
    registration: &'a ResourceControllerRegistration,
    catalog: &'a dill::Catalog,
    failure_backoff: &'a RetryBackoff,
}

impl<'a> ResourceControllerOutcomeRecorder<'a> {
    pub fn new(
        registration: &'a ResourceControllerRegistration,
        catalog: &'a dill::Catalog,
        failure_backoff: &'a RetryBackoff,
    ) -> Self {
        Self {
            registration,
            catalog,
            failure_backoff,
        }
    }

    pub async fn record(
        &self,
        id: ResourceID,
        attempt: &ResourceProcessAttempt,
        now: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let meta = self.registration.meta();

        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |repository: Arc<dyn ResourceControllerStateRepository>| async move {
                    // The previous row carries the failure count and what the
                    // controller handled before, which a retry or failure keeps
                    let previous = repository.get_state(&meta.controller_name, &id).await?;
                    let state =
                        attempt.next_state(meta, id, previous.as_ref(), now, self.failure_backoff);
                    repository.upsert_state(&state).await
                },
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
