// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Type implementing this trait will be called during the health check
/// procedure. Multiple checks can be added, in which case they will be called
/// one by one with check considered failed upon the first error.
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check(&self, check_type: CheckType) -> Result<(), CheckError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Axum handler for serving the health checks. Depends on [`dill::Catalog`] to
/// instantiate the implementations of the [`HealthCheck`] trait.
#[cfg(feature = "dill")]
pub async fn health_handler(
    axum::Extension(catalog): axum::Extension<dill::Catalog>,
    axum::extract::Query(args): axum::extract::Query<CheckArgs>,
) -> Result<axum::Json<CheckSuccess>, CheckError> {
    for checker in catalog.get::<dill::AllOf<dyn HealthCheck>>().unwrap() {
        checker.check(args.r#type).await?;
    }

    Ok(axum::Json(CheckSuccess { ok: true }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckSuccess {
    pub ok: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct CheckError {
    pub reason: String,
}

impl axum::response::IntoResponse for CheckError {
    fn into_response(self) -> axum::response::Response {
        (
            http::status::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "ok": false,
                "reason": self.reason,
            })),
        )
            .into_response()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckType {
    /// Determines when to restart the container
    #[default]
    Liveness,
    /// Determines when to remove the instance from the loadbalancer
    Readiness,
    /// Is sent before liveness and readiness checks to determine when the
    /// startup procedure is finished procedure is finished
    Startup,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
pub struct CheckArgs {
    #[serde(default)]
    pub r#type: CheckType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
