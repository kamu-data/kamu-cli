// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Another transaction committed a key this transaction changed, after this
/// transaction first changed it
#[derive(Error, Debug)]
#[error("In-memory write conflict on key {key}")]
pub struct InMemoryWriteConflictError {
    pub key: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
