// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::FlowScope;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FlowBinding {
    pub flow_type: String,
    pub scope: FlowScope,
}

impl FlowBinding {
    #[inline]
    pub fn new(flow_type: &str, scope: FlowScope) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowBindingStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<FlowBinding, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
