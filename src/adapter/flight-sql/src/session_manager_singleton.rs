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

use crate::{internal_error, PlanId, SessionManager};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This basic session manager is for testing use only. It creates context
/// once and reuses it for all the following operations. Once created the
/// context is never released, thus all resources associated with table
/// providers will remain tied up until the entire Flight SQL service is cleaned
/// up.
#[dill::component]
#[dill::interface(dyn SessionManager)]
pub struct SessionManagerSingleton {
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
    plans: HashMap<PlanId, LogicalPlan>,
}

#[async_trait::async_trait]
impl SessionManager for SessionManagerSingleton {
    async fn close_session(&self) -> Result<(), Status> {
        let mut state = self.state.inner.lock().unwrap();
        state.take();
        Ok(())
    }

    async fn get_context(&self) -> Result<Arc<SessionContext>, Status> {
        {
            let state = self.state.inner.lock().unwrap();
            if let Some(state) = &(*state) {
                return Ok(Arc::clone(&state.ctx));
            }
        }

        let query_svc = self.query_svc.get().map_err(internal_error)?;

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

    async fn cache_plan(&self, plan: LogicalPlan) -> Result<PlanId, Status> {
        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        let plan_token = PlanId(uuid::Uuid::new_v4().to_string());
        s.plans.insert(plan_token.clone(), plan);

        Ok(plan_token)
    }

    async fn get_plan(&self, plan_id: &PlanId) -> Result<LogicalPlan, Status> {
        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        let Some(plan) = s.plans.get(plan_id) else {
            return Err(Status::internal("No such plan"));
        };

        Ok(plan.clone())
    }

    async fn remove_plan(&self, plan_id: &PlanId) -> Result<(), Status> {
        let mut state = self.state.inner.lock().unwrap();
        let Some(s) = &mut (*state) else {
            return Err(Status::internal("Invalid state"));
        };

        s.plans.remove(plan_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
