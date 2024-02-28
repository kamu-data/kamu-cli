// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::InternalError;
use kamu_flow_system::{FlowID, FlowKey, FlowResult};

use super::FlowServiceCallbacksFacade;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait FlowTypeSupportStrategy: Send + Sync {
    async fn on_flow_success(
        &self,
        facade: &dyn FlowServiceCallbacksFacade,
        success_time: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: &FlowKey,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
