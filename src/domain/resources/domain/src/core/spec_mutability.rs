// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What applying a different spec to an existing resource of a type leads to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecMutability {
    /// The spec is updated
    Mutable,
    /// The apply is rejected: the spec is written once, at creation
    Immutable,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
