// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An untyped link names a different type than the typed link it is converted
/// into.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("Expected a link to a resource of type {expected}, got {actual}")]
pub struct ResourceTypeMismatchError {
    pub expected: TypeUri,
    pub actual: TypeRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
