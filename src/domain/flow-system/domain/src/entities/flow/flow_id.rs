// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::TryFromIntError;

use internal_error::InternalError;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a flow
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FlowID(u64);

impl FlowID {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn from(id_as_str: &str) -> Result<Self, std::num::ParseIntError> {
        let id = id_as_str.parse()?;
        Ok(Self(id))
    }
}

impl TryFrom<i64> for FlowID {
    type Error = TryFromIntError;

    fn try_from(val: i64) -> Result<Self, Self::Error> {
        let id: u64 = u64::try_from(val)?;
        Ok(Self::new(id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for FlowID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for FlowID {
    fn from(val: u64) -> Self {
        Self::new(val)
    }
}

impl From<FlowID> for u64 {
    fn from(val: FlowID) -> Self {
        val.0
    }
}

impl TryFrom<FlowID> for i64 {
    type Error = TryFromIntError;

    fn try_from(val: FlowID) -> Result<Self, Self::Error> {
        i64::try_from(val.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<FlowID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
