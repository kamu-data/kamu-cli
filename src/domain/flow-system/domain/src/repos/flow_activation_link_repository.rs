// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FlowActivationLink, FlowID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowActivationLinkRepository: Send + Sync {
    /// Stores a link. Idempotent: an existing pair of flows keeps its original
    /// link unchanged.
    async fn save_link(&self, link: &FlowActivationLink) -> Result<(), InternalError>;

    /// Links leaving any of the given upstream flows, ordered by upstream flow,
    /// activation time, then downstream flow
    async fn get_downstream_links(
        &self,
        upstream_flow_ids: &[FlowID],
    ) -> Result<Vec<FlowActivationLink>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
