// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryOutboxMessageConsumptionRepository {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    consumption_boundaries: HashMap<String, OutboxMessageConsumptionBoundary>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn OutboxMessageConsumptionRepository)]
impl InMemoryOutboxMessageConsumptionRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn make_key(&self, consumer_name: &str, producer_name: &str) -> String {
        format!("{consumer_name}<->{producer_name}")
    }
}

#[async_trait::async_trait]
impl OutboxMessageConsumptionRepository for InMemoryOutboxMessageConsumptionRepository {
    fn list_consumption_boundaries(&self) -> OutboxMessageConsumptionBoundariesStream {
        let boundaries = {
            let guard = self.state.lock().unwrap();
            guard
                .consumption_boundaries
                .values()
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>()
        };

        Box::pin(tokio_stream::iter(boundaries))
    }

    async fn find_consumption_boundary(
        &self,
        consumer_name: &str,
        producer_name: &str,
    ) -> Result<Option<OutboxMessageConsumptionBoundary>, InternalError> {
        let key = self.make_key(consumer_name, producer_name);
        let guard = self.state.lock().unwrap();
        Ok(guard.consumption_boundaries.get(&key).cloned())
    }

    async fn create_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), CreateConsumptionBoundaryError> {
        let key = self.make_key(&boundary.consumer_name, &boundary.producer_name);

        let mut guard = self.state.lock().unwrap();

        if let Entry::Vacant(e) = guard.consumption_boundaries.entry(key) {
            e.insert(boundary);
            Ok(())
        } else {
            Err(
                CreateConsumptionBoundaryError::DuplicateConsumptionBoundary(
                    DuplicateConsumptionBoundaryError {
                        consumer_name: boundary.consumer_name.to_string(),
                        producer_name: boundary.producer_name.to_string(),
                    },
                ),
            )
        }
    }

    async fn update_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), UpdateConsumptionBoundaryError> {
        let key = self.make_key(&boundary.consumer_name, &boundary.producer_name);

        let mut guard = self.state.lock().unwrap();

        let maybe_boundary = guard.consumption_boundaries.get_mut(&key);
        if let Some(existing_boundary) = maybe_boundary {
            existing_boundary.last_consumed_message_id = boundary.last_consumed_message_id;
            Ok(())
        } else {
            Err(UpdateConsumptionBoundaryError::ConsumptionBoundaryNotFound(
                ConsumptionBoundaryNotFoundError {
                    consumer_name: boundary.consumer_name.to_string(),
                    producer_name: boundary.producer_name.to_string(),
                },
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
