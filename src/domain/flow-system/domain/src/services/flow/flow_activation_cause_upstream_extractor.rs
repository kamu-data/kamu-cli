// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{FlowActivationCauseResourceUpdate, FlowID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reads the upstream flow out of a resource update cause of one resource type,
/// whose details the flow system cannot interpret itself
pub trait FlowActivationCauseUpstreamExtractor: Send + Sync {
    /// The `resource_type` of causes this extractor understands
    fn resource_type(&self) -> &'static str;

    /// The flow that produced the update; `None` when the update did not come
    /// from a flow or its details cannot be read
    fn extract_upstream_flow_id(
        &self,
        update: &FlowActivationCauseResourceUpdate,
    ) -> Option<FlowID>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
