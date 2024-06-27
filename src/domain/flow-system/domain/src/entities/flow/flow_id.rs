// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a flow
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlowID(u64);

impl FlowID {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for FlowID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<FlowID> for u64 {
    fn from(val: FlowID) -> Self {
        val.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowIDStream<'a> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<FlowID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
