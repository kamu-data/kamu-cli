// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum InvalidSchema {
    #[error(transparent)]
    Malformed(#[from] MalformedSchema),

    #[error(transparent)]
    Unsupported(#[from] UnsupportedSchema),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct MalformedSchema {
    message: String,
}

impl MalformedSchema {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct UnsupportedSchema {
    message: String,
}

impl UnsupportedSchema {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
