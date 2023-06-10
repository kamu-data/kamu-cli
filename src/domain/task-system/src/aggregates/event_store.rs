// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::errors::*;
use crate::Aggregate;

/////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event store
#[async_trait::async_trait]
pub trait EventStore
where
    Self: Send + Sync,
    Self::Agg: Send,
    //Self::EventInstance: Into<<Self::Agg as Aggregate>::Event>,
    <Self::Agg as Aggregate>::Event: Send,
{
    type Agg: Aggregate;
    type EventInstance;

    /// Initializes an aggregate from event history
    async fn load(
        &self,
        id: &<Self::Agg as Aggregate>::Id,
    ) -> Result<Self::Agg, LoadError<Self::Agg>>;

    /// Persists pending aggregate events
    async fn save(&self, agg: &mut Self::Agg) -> Result<(), SaveError> {
        let events = agg.updates();
        self.save_events(events).await
    }

    // TODO: Concurrency control
    /// Persists a singular event
    async fn save_event(&self, event: <Self::Agg as Aggregate>::Event) -> Result<(), SaveError>;

    // TODO: Concurrency control
    /// Persists a series of events
    async fn save_events(
        &self,
        events: Vec<<Self::Agg as Aggregate>::Event>,
    ) -> Result<(), SaveError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
