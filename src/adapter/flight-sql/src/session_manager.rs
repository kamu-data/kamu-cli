// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use tonic::Status;

use crate::{PlanToken, SessionToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Responsible for managing the state associated with the client session.
#[async_trait::async_trait]
pub trait SessionManager: Send + Sync {
    /// Called during the handshake stage to authenticate the client.
    ///
    /// Returns a bearer token by which the client is associated with its
    /// session in all subsequent calls.
    async fn auth_basic(&self, username: &str, password: &str) -> Result<SessionToken, Status>;

    /// Can be used to free the client session resources and state when
    /// connection is gracefully closed.
    async fn end_session(&self, token: &SessionToken) -> Result<(), Status>;

    /// Called on every operation to get the session context for the client.
    /// Token argument represents the token returned at the authentication
    /// stage.
    ///
    /// Note that the session token should be treated as untrusted - it's the
    /// job of session manager implementation to verify it before returning
    /// the context.
    async fn get_context(&self, token: &SessionToken) -> Result<Arc<SessionContext>, Status>;

    /// Called to cache the logical plan of a prepared statement
    async fn cache_plan(
        &self,
        token: &SessionToken,
        plan: LogicalPlan,
    ) -> Result<PlanToken, Status>;

    /// Called to retrieve the previously cached logical plan of a prepared
    /// statement
    async fn get_plan(
        &self,
        token: &SessionToken,
        plan_token: &PlanToken,
    ) -> Result<LogicalPlan, Status>;

    /// Called to clean up the previously cached logical plan
    async fn remove_plan(&self, token: &SessionToken, plan_token: &PlanToken)
        -> Result<(), Status>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
