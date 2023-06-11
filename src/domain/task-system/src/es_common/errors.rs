// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::Aggregate;

/////////////////////////////////////////////////////////////////////////////////////////
// LoadError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum LoadError<Agg: Aggregate> {
    #[error(transparent)]
    NotFound(#[from] AggrateNotFoundError<Agg>),
    #[error(transparent)]
    ProjectionError(ProjectionError<Agg>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

// Transitive From
impl<Agg: Aggregate, Err: Into<ProjectionError<Agg>>> From<Err> for LoadError<Agg> {
    fn from(value: Err) -> Self {
        Self::ProjectionError(value.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// SaveError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SaveError {
    // TODO: Concurrency
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////
// ProjectionError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ProjectionError<Agg: Aggregate> {
    #[error(transparent)]
    IllegalGenesis(#[from] IllegalGenesisError<Agg>),
    #[error(transparent)]
    IllegalSequence(#[from] IllegalSequenceError<Agg>),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Cannot initialize projection from {event:?}")]
pub struct IllegalGenesisError<Agg: Aggregate> {
    pub event: <Agg as Aggregate>::Event,
}

impl<Agg: Aggregate> IllegalGenesisError<Agg> {
    pub fn new(event: <Agg as Aggregate>::Event) -> Self {
        Self { event }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Event {event:?} is illegal for state {state:?}")]
pub struct IllegalSequenceError<Agg: Aggregate> {
    pub state: <Agg as Aggregate>::State,
    pub event: <Agg as Aggregate>::Event,
}

impl<Agg: Aggregate> IllegalSequenceError<Agg> {
    pub fn new(state: <Agg as Aggregate>::State, event: <Agg as Aggregate>::Event) -> Self {
        Self { state, event }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("{id:?} not found")]
pub struct AggrateNotFoundError<Agg: Aggregate> {
    pub id: <Agg as Aggregate>::Id,
}

impl<Agg: Aggregate> AggrateNotFoundError<Agg> {
    pub fn new(id: <Agg as Aggregate>::Id) -> Self {
        Self { id }
    }
}
