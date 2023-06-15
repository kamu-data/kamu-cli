// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Projections reconstruct some state from a series of events
#[async_trait::async_trait]
#[allow(drop_bounds)]
pub trait Projection
where
    Self: Sized + Send + Sync + 'static,
    Self::Query: Sized + Send + Sync + 'static,
    Self::Event: Sized + Send + Sync + 'static,

    Self: Clone,
    Self::Query: Clone,
    Self::Event: Clone,

    Self: std::fmt::Debug,
    Self::Query: std::fmt::Debug,
    Self::Event: std::fmt::Debug,
{
    /// Type of the query this projection uses to filter events in the event
    /// store
    type Query;

    /// Type of the event this projection considers
    type Event;

    /// Update state projection with an event
    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// ProjectionError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub struct ProjectionError<Proj: Projection> {
    pub state: Option<Proj>,
    pub event: <Proj as Projection>::Event,
}

impl<Proj: Projection> ProjectionError<Proj> {
    pub fn new(state: Option<Proj>, event: <Proj as Projection>::Event) -> Self {
        Self { state, event }
    }
}

impl<Proj: Projection> std::fmt::Display for ProjectionError<Proj> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(state) = &self.state {
            write!(f, "Event {:?} is illegal for state {:?}", self.event, state)
        } else {
            write!(
                f,
                "Cannot initialize {} from event {:?}",
                std::any::type_name::<Proj>(),
                self.event
            )
        }
    }
}
