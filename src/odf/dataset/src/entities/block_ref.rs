// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CheapClone for BlockRef {}

/// References are named pointers to metadata blocks
#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Debug,
    strum::Display,
    strum::EnumString,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[strum(
    serialize_all = "lowercase",
    parse_err_ty = InternalError,
    parse_err_fn = invalid_block_ref
)]
pub enum BlockRef {
    Head,
}

impl BlockRef {
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

fn invalid_block_ref(s: &str) -> InternalError {
    format!("Invalid block reference: {s}").int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
