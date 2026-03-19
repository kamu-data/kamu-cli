// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ProjectionError;
use thiserror::Error;

use crate::{VariableSetState, VariableSetValidationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VariableSetLifecycleError {
    #[error(transparent)]
    Validation(#[from] VariableSetValidationError),

    #[error("variable set with identical spec already exists")]
    NoChanges,

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<VariableSetState>>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
