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

use chrono::{DateTime, Utc};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use kamu_core::QueryService;
use time_source::SystemTimeSource;
use tonic::Status;

use crate::{PlanToken, SessionAuth, SessionManager, SessionToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SessionCachingConfig {
    pub session_expiration_timeout: std::time::Duration,
    pub session_inactivity_timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SessionManagerCachingState {
    sessions: Mutex<HashMap<SessionToken, Session>>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl SessionManagerCachingState {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }
}

struct Session {
    ctx: Option<Arc<SessionContext>>,
    plans: HashMap<PlanToken, LogicalPlan>,
    created: DateTime<Utc>,
    accessed: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This session manager uses in-memory cache of session contexts. Contexts are
/// cached for an _inactivity period_ of time starting from the last use and are
/// automatically cleaned up. Accessing an inactive session will re-create the
/// context. Sessions themselves have _expiration period_ after which the client
/// requests will be rejected and they have to re-authenticate.
#[dill::component(pub)]
#[dill::interface(dyn SessionManager)]
pub struct SessionManagerCaching {
    conf: Arc<SessionCachingConfig>,
    timer: Arc<dyn SystemTimeSource>,
    auth: dill::Lazy<Arc<dyn SessionAuth>>,
    query_svc: dill::Lazy<Arc<dyn QueryService>>,
    state: Arc<SessionManagerCachingState>,
}

impl SessionManagerCaching {
    async fn create_context(&self) -> Result<Arc<SessionContext>, Status> {
        let query_svc = self
            .query_svc
            .get()
            .map_err(|_| Status::internal("Injection error"))?;

        let ctx = query_svc
            .create_session()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Arc::new(ctx))
    }

    fn schedule_expiration(&self, token: SessionToken) {
        let expires_in = self.conf.session_expiration_timeout;
        let state = Arc::clone(&self.state);

        tokio::task::spawn(async move {
            tokio::time::sleep(expires_in).await;
            let mut sessions = state.sessions.lock().unwrap();
            if let Some(session) = sessions.remove(&token) {
                tracing::debug!(
                    token,
                    created_at = %session.created,
                    "Expiring FlightSQL session",
                );
            }
        });
    }

    fn schedule_inactivity(&self, token: SessionToken, for_accessed_at: DateTime<Utc>) {
        let timer = self.timer.clone();
        let inactivity_timeout = self.conf.session_inactivity_timeout;
        let state = Arc::clone(&self.state);

        tokio::task::spawn(async move {
            let mut accessed_cas = for_accessed_at;
            let mut inactive_in = inactivity_timeout;

            loop {
                tokio::time::sleep(inactive_in).await;

                let mut sessions = state.sessions.lock().unwrap();
                let Some(session) = sessions.get_mut(&token) else {
                    // Session expired
                    break;
                };

                if session.accessed == accessed_cas {
                    // Session was not accessed since this timer was scheduled - release the context
                    tracing::debug!(
                        token,
                        created_at = %session.created,
                        accessed_at = %session.accessed,
                        ?inactivity_timeout,
                        "Releasing inactive FlightSQL session context",
                    );
                    session.ctx.take();
                    break;
                }

                // Re-schedule to updated time
                accessed_cas = session.accessed;
                inactive_in =
                    inactivity_timeout - (timer.now() - session.accessed).to_std().unwrap();
            }
        });
    }
}

#[async_trait::async_trait]
impl SessionManager for SessionManagerCaching {
    async fn auth_basic(&self, username: &str, password: &str) -> Result<SessionToken, Status> {
        self.auth
            .get()
            .map_err(|_| Status::internal("Injection error"))?
            .auth_basic(username, password)
            .await?;

        let now = self.timer.now();
        let token = SessionToken::from(uuid::Uuid::new_v4());

        tracing::debug!(token, "Creating new FlightSQL session");

        let session = Session {
            ctx: Some(self.create_context().await?),
            plans: HashMap::new(),
            created: now,
            accessed: now,
        };

        let mut sessions = self.state.sessions.lock().unwrap();
        sessions.insert(token.clone(), session);

        self.schedule_inactivity(token.clone(), now);
        self.schedule_expiration(token.clone());

        Ok(token)
    }

    async fn end_session(&self, token: &SessionToken) -> Result<(), Status> {
        let mut sessions = self.state.sessions.lock().unwrap();
        sessions.remove(token);
        Ok(())
    }

    async fn get_context(&self, token: &SessionToken) -> Result<Arc<SessionContext>, Status> {
        // Try to get existing context
        {
            let mut sessions = self.state.sessions.lock().unwrap();

            let Some(session) = sessions.get_mut(token) else {
                return Err(Status::unauthenticated("Invalid token"));
            };

            if let Some(ctx) = &session.ctx {
                tracing::debug!(token, "Reusing FlightSQL session context");

                // The inactivity timer will reschedule itself
                session.accessed = self.timer.now();
                return Ok(Arc::clone(ctx));
            }
        }

        // Session was inactive - re-create the context
        tracing::debug!(token, "Re-creating suspended FlightSQL session context");

        let ctx = self.create_context().await?;

        {
            let mut sessions = self.state.sessions.lock().unwrap();

            let Some(session) = sessions.get_mut(token) else {
                return Err(Status::unauthenticated("Invalid token"));
            };

            session.accessed = self.timer.now();

            if let Some(ctx) = &session.ctx {
                // Oops, another thread created the context already - reuse the existing one
                Ok(Arc::clone(ctx))
            } else {
                // Insert new context into the session
                session.ctx = Some(Arc::clone(&ctx));

                // Schedule inactivity timer
                self.schedule_inactivity(token.clone(), session.accessed);

                Ok(ctx)
            }
        }
    }

    async fn cache_plan(
        &self,
        token: &SessionToken,
        plan: LogicalPlan,
    ) -> Result<PlanToken, Status> {
        let mut sessions = self.state.sessions.lock().unwrap();

        let Some(session) = sessions.get_mut(token) else {
            return Err(Status::unauthenticated("Invalid token"));
        };

        let plan_token = PlanToken::from(uuid::Uuid::new_v4());
        session.plans.insert(plan_token.clone(), plan);

        Ok(plan_token)
    }

    async fn get_plan(
        &self,
        token: &SessionToken,
        plan_token: &PlanToken,
    ) -> Result<LogicalPlan, Status> {
        let sessions = self.state.sessions.lock().unwrap();

        let Some(session) = sessions.get(token) else {
            return Err(Status::unauthenticated("Invalid token"));
        };

        let Some(plan) = session.plans.get(plan_token) else {
            return Err(Status::unauthenticated("Invalid plan token"));
        };

        Ok(plan.clone())
    }

    async fn remove_plan(
        &self,
        token: &SessionToken,
        plan_token: &PlanToken,
    ) -> Result<(), Status> {
        let mut sessions = self.state.sessions.lock().unwrap();

        let Some(session) = sessions.get_mut(token) else {
            return Err(Status::unauthenticated("Invalid token"));
        };

        session.plans.remove(plan_token);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
