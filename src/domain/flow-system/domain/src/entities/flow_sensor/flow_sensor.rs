// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::{FlowActivationCause, FlowBinding, FlowScope};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSensor: Send + Sync {
    fn flow_scope(&self) -> &FlowScope;

    fn get_sensitive_to_scopes(&self) -> Vec<FlowScope>;

    async fn on_activated(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError>;

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: &FlowActivationCause,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
