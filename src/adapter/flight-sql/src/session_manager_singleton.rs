// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use kamu_core::QueryService;
use tonic::Status;

use crate::{PlanToken, SessionAuth, SessionManager, SessionToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This basic session manager is for testing use only. It creates context
/// once and reuses it for all the folowing operations. Once created the context
/// is never released, thus all resources associated with table providers will
/// remain tied up until the entire Flight SQL service is cleaned up.
#[dill::component]
#[dill::interface(dyn SessionManager)]
pub struct SessionManagerSingleton {
    auth: dill::Lazy<Arc<dyn SessionAuth>>,
    query_svc: dill::Lazy<Arc<dyn QueryService>>,
    state: Arc<SessionManagerSingletonState>,
}

pub struct SessionManagerSingletonState {
    inner: Mutex<Option<Inner>>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl SessionManagerSingletonState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }
}

struct Inner {
    ctx: Arc<SessionContext>,
    plans: HashMap<PlanToken, LogicalPlan>,
}

impl SessionManagerSingleton {
    const DUMMY_TOKEN: &'static str = "singleton-token";

    fn check_token(&self, token: &SessionToken) -> Result<(), Status> {
        if token == Self::DUMMY_TOKEN {
            Ok(())
        } else {
            Err(Status::unauthenticated("Invalid token"))
        }
    }
}

#[async_trait::async_trait]
impl SessionManager for SessionManagerSingleton {
    async fn auth_basic(&self, username: &str, password: &str) -> Result<SessionToken, Status> {
        self.auth
            .get()
            .map_err(|_| Status::internal("Injection error"))?
            .auth_basic(username, password)
            .await?;

        Ok(SessionToken::from(Self::DUMMY_TOKEN))
    }

    async fn end_session(&self, token: &SessionToken) -> Result<(), Status> {
        self.check_token(token)?;

        let mut state = self.state.inner.lock().unwrap();
        state.take();
        Ok(())
    }

    async fn get_context(&self, token: &SessionToken) -> Result<Arc<SessionContext>, Status> {
        self.check_token(token)?;

        {
            let state = self.state.inner.lock().unwrap();
            if let Some(state) = &(*state) {
                return Ok(Arc::clone(&state.ctx));
            }
        }

        let query_svc = self
            .query_svc
            .get()
            .map_err(|_| Status::internal("Injection error"))?;

        let ctx = query_svc
            .create_session()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let ctx = Arc::new(ctx);

        *self.state.inner.lock().unwrap() = Some(Inner {
            ctx: Arc::clone(&ctx),
            plans: HashMap::new(),
        });

        Ok(ctx)
    }

    async fn cache_plan(
        &self,
        token: &SessionToken,
        plan: LogicalPlan,
    ) -> Result<PlanToken, Status> {
        self.check_token(token)?;

        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        let plan_token = PlanToken::from(uuid::Uuid::new_v4());
        s.plans.insert(plan_token.clone(), plan);

        Ok(plan_token)
    }

    async fn get_plan(
        &self,
        token: &SessionToken,
        plan_token: &PlanToken,
    ) -> Result<LogicalPlan, Status> {
        self.check_token(token)?;

        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        let Some(plan) = s.plans.get(plan_token) else {
            return Err(Status::internal("No such plan"));
        };

        Ok(plan.clone())
    }

    async fn remove_plan(
        &self,
        token: &SessionToken,
        plan_token: &PlanToken,
    ) -> Result<(), Status> {
        self.check_token(token)?;

        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        s.plans.remove(plan_token);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
