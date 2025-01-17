// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// References are named pointers to metadata blocks
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum BlockRef {
    Head,
}

impl BlockRef {
    pub fn as_str(&self) -> &str {
        match self {
            BlockRef::Head => "head",
        }
    }
}

impl std::str::FromStr for BlockRef {
    type Err = InternalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "head" => Ok(Self::Head),
            _ => Err(format!("Invalid block reference: {s}").int_err()),
        }
    }
}

impl AsRef<str> for BlockRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for BlockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
