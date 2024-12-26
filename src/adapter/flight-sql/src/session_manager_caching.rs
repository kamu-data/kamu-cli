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
use kamu_accounts::CurrentAccountSubject;
use kamu_core::QueryService;
use time_source::SystemTimeSource;
use tonic::Status;

use crate::{internal_error, PlanId, SessionId, SessionManager};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SessionCachingConfig {
    pub authed_session_expiration_timeout: std::time::Duration,
    pub authed_session_inactivity_timeout: std::time::Duration,
    pub anon_session_expiration_timeout: std::time::Duration,
    pub anon_session_inactivity_timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SessionManagerCachingState {
    sessions: Mutex<HashMap<SessionId, Session>>,
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
    plans: HashMap<PlanId, LogicalPlan>,
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
    subject: Arc<CurrentAccountSubject>,
    session_id: SessionId,
    conf: Arc<SessionCachingConfig>,
    timer: Arc<dyn SystemTimeSource>,
    query_svc: dill::Lazy<Arc<dyn QueryService>>,
    state: Arc<SessionManagerCachingState>,
}

impl SessionManagerCaching {
    fn get_or_create_session<F, R>(&self, fun: F) -> Result<R, Status>
    where
        F: FnOnce(&mut Session) -> Result<R, Status>,
    {
        let mut sessions = self.state.sessions.lock().unwrap();

        let session = if let Some(session) = sessions.get_mut(&self.session_id) {
            session
        } else {
            tracing::debug!("Creating new FlightSQL session");
            let now = self.timer.now();

            let session = Session {
                ctx: None,
                plans: HashMap::new(),
                created: now,
                accessed: now,
            };

            sessions.insert(self.session_id.clone(), session);

            self.schedule_expiration();

            sessions.get_mut(&self.session_id).unwrap()
        };

        fun(session)
    }

    async fn create_context(&self) -> Result<Arc<SessionContext>, Status> {
        let query_svc = self.query_svc.get().map_err(internal_error)?;

        let ctx = query_svc.create_session().await.map_err(internal_error)?;

        Ok(Arc::new(ctx))
    }

    fn schedule_expiration(&self) {
        let session_id = self.session_id.clone();
        let expires_in = match self.subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => self.conf.anon_session_expiration_timeout,
            CurrentAccountSubject::Logged(_) => self.conf.authed_session_expiration_timeout,
        };
        let state = Arc::clone(&self.state);

        tokio::task::spawn(async move {
            tokio::time::sleep(expires_in).await;
            let mut sessions = state.sessions.lock().unwrap();
            if let Some(session) = sessions.remove(&session_id) {
                tracing::debug!(
                    %session_id,
                    created_at = %session.created,
                    "Expiring FlightSQL session",
                );
            }
        });
    }

    fn schedule_inactivity(&self, for_accessed_at: DateTime<Utc>) {
        let session_id = self.session_id.clone();
        let timer = self.timer.clone();
        let inactivity_timeout = match self.subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => self.conf.anon_session_inactivity_timeout,
            CurrentAccountSubject::Logged(_) => self.conf.authed_session_inactivity_timeout,
        };
        let state = Arc::clone(&self.state);

        tokio::task::spawn(async move {
            let mut accessed_cas = for_accessed_at;
            let mut inactive_in = inactivity_timeout;

            loop {
                tokio::time::sleep(inactive_in).await;

                let mut sessions = state.sessions.lock().unwrap();
                let Some(session) = sessions.get_mut(&session_id) else {
                    // Session expired
                    break;
                };

                if session.accessed == accessed_cas {
                    // Session was not accessed since this timer was scheduled - release the context
                    tracing::debug!(
                        %session_id,
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
                inactive_in = inactivity_timeout
                    - (std::cmp::max(timer.now(), session.accessed) - session.accessed)
                        .to_std()
                        .unwrap();
            }
        });
    }
}

#[async_trait::async_trait]
impl SessionManager for SessionManagerCaching {
    async fn close_session(&self) -> Result<(), Status> {
        let mut sessions = self.state.sessions.lock().unwrap();
        if sessions.remove(&self.session_id).is_some() {
            tracing::debug!(
                session_id = %self.session_id,
                "Closed FlightSQL session context"
            );
        }
        Ok(())
    }

    async fn get_context(&self) -> Result<Arc<SessionContext>, Status> {
        // Try to get existing context
        if let Some(ctx) = self.get_or_create_session(|session| {
            if let Some(ctx) = &session.ctx {
                // The inactivity timer will reschedule itself
                session.accessed = self.timer.now();
                Ok(Some(Arc::clone(ctx)))
            } else {
                Ok(None)
            }
        })? {
            tracing::debug!(
                session_id = %self.session_id,
                "Reusing FlightSQL session context"
            );
            return Ok(ctx);
        }

        // Session was inactive - re-create the context
        tracing::debug!(
            session_id = %self.session_id,
            "Creating FlightSQL session context"
        );

        let new_ctx = self.create_context().await?;

        self.get_or_create_session(move |session| {
            if let Some(ctx) = &session.ctx {
                // Oops, another thread created the context already - reuse the existing one
                Ok(Arc::clone(ctx))
            } else {
                // Insert new context into the session
                session.ctx = Some(Arc::clone(&new_ctx));

                // Schedule inactivity timer
                self.schedule_inactivity(session.accessed);

                Ok(new_ctx)
            }
        })
    }

    async fn cache_plan(&self, plan: LogicalPlan) -> Result<PlanId, Status> {
        let plan_id = PlanId(uuid::Uuid::new_v4().to_string());

        self.get_or_create_session(move |session| {
            session.plans.insert(plan_id.clone(), plan);
            Ok(plan_id)
        })
    }

    async fn get_plan(&self, plan_id: &PlanId) -> Result<LogicalPlan, Status> {
        self.get_or_create_session(move |session| {
            if let Some(plan) = session.plans.get(plan_id) {
                Ok(plan.clone())
            } else {
                Err(Status::unauthenticated("Invalid plan token"))
            }
        })
    }

    async fn remove_plan(&self, plan_id: &PlanId) -> Result<(), Status> {
        self.get_or_create_session(move |session| {
            session.plans.remove(plan_id);
            Ok(())
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
