// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::errors::*;
use crate::EventID;

/////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event-sourced aggregate
#[async_trait::async_trait]
#[allow(drop_bounds)]
pub trait Aggregate
where
    Self: Sized,
    // Aggregate state should be directly accessible
    Self: std::ops::Deref<Target = Self::State>,
    // Aggregate can be passed into functions that expect state
    Self: AsRef<Self::State>,
    // Aggregate should be convertable into the state (must panic if there are pending events)
    Self: Into<Self::State>,
    // Aggregates should should not be dropped without saving pending events (must log an error
    // otherwise)
    Self: Drop,
    Self::Id: std::fmt::Debug + std::fmt::Display,
    Self::Event: std::fmt::Debug,
    Self::State: std::fmt::Debug,
    Self::Id: Clone + Send + Sync,
    Self::Event: Clone + Send,
    Self::State: Clone + Send,
{
    /// Type of the aggregate's identity
    type Id;
    /// Type of the event associated with an aggregate
    type Event;
    /// Type of the state maintained by an aggregate
    type State;

    /// Unique identity of an aggregate
    fn id(&self) -> &Self::Id;

    /// Initializes an aggregate from projected state
    fn from_genesis_event(
        event_id: EventID,
        event: Self::Event,
    ) -> Result<Self, IllegalGenesisError<Self>>;

    /// Initializes an aggregate from a state snapshot
    fn from_snapshot(event_id: EventID, state: Self::State) -> Self;

    /// Update current state projection with an event
    fn mutate(
        &mut self,
        event_id: EventID,
        event: Self::Event,
    ) -> Result<(), IllegalSequenceError<Self>>;

    /// Checks whether an aggregate has pending updates that need to be saved
    fn has_updates(&self) -> bool;

    /// Called by [crate::EventStore] to extracts all pending updates
    fn updates(&mut self) -> Vec<Self::Event>;

    /// Returns the ID corresponding to the last event that reliably stored in
    /// an event store
    fn last_synced_event(&self) -> Option<&EventID>;

    /// Called by [crate::EventStore] to update the last synced event ID
    fn update_last_synced_event(&mut self, event_id: EventID);
}
