// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::BoxedError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AccessError {
    #[error("Resource is read-only")]
    ReadOnly(#[source] Option<BoxedError>),
    #[error("Unauthorized")]
    Unauthorized(#[source] BoxedError),
    #[error("Forbidden")]
    Forbidden(#[source] BoxedError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
