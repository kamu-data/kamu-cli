// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////
// LoadError
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum LoadError<Proj, Evt> {
    #[error(transparent)]
    ProjectionError(#[from] ProjectionError<Proj, Evt>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl<Proj, Evt> From<IllegalSequenceError<Proj, Evt>> for LoadError<Proj, Evt> {
    fn from(value: IllegalSequenceError<Proj, Evt>) -> Self {
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

use internal_error::InternalError;

#[derive(thiserror::Error, Debug)]
pub enum ProjectionError<Proj, Evt> {
    #[error(transparent)]
    IllegalGenesis(#[from] IllegalGenesisError<Evt>),
    #[error(transparent)]
    IllegalSequence(#[from] IllegalSequenceError<Proj, Evt>),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Cannot initialize projection from {event:?}")]
pub struct IllegalGenesisError<Evt> {
    pub event: Evt,
}

impl<Evt> IllegalGenesisError<Evt> {
    pub fn new(event: Evt) -> Self {
        Self { event }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Event {event:?} is illegal for state {state:?}")]
pub struct IllegalSequenceError<Proj, Evt> {
    pub state: Proj,
    pub event: Evt,
}

impl<Proj, Evt> IllegalSequenceError<Proj, Evt> {
    pub fn new(state: Proj, event: Evt) -> Self {
        Self { state, event }
    }
}
