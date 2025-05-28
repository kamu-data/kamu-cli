// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::{Backtrace, BacktraceStatus};
use std::sync::Arc;

use async_graphql::extensions::*;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This GQL extension (middleware) will log the important information about the
/// request using `tracing` library. Unlike the similar extension provided by
/// `async_graphql` it will log the error message and backtrace of failed
/// resolvers.
pub struct Tracing;

impl async_graphql::extensions::ExtensionFactory for Tracing {
    fn create(&self) -> Arc<dyn async_graphql::extensions::Extension> {
        Arc::new(TracingExtension)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TracingExtension;

#[async_trait::async_trait]
impl async_graphql::extensions::Extension for TracingExtension {
    #[tracing::instrument(level = "info", name = "GQL: request", skip_all)]
    async fn request(
        &self,
        ctx: &ExtensionContext<'_>,
        next: NextRequest<'_>,
    ) -> async_graphql::Response {
        let response = next.run(ctx).await;
        for err in &response.errors {
            if let Some(source) = err.source::<InternalError>() {
                tracing::error!(
                    error = ?source,
                    error_msg = %ErrorMessageFormatter(source),
                    error_backtrace = %ErrorBacktraceFormatter(source),
                    gql_path = ?err.path,
                    "Unhandled error",
                );
            } else if let Some(source) = err.source::<odf::AccessError>() {
                tracing::warn!(
                    error = ?source,
                    error_msg = %ErrorMessageFormatter(source),
                    error_backtrace = %ErrorBacktraceFormatter(source),
                    gql_path = ?err.path,
                    "Access error",
                );
            } else {
                tracing::error!(
                    error_msg = %err.message,
                    gql_path = ?err.path,
                    "Unhandled generic error",
                );
            }
        }
        response
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ErrorMessageFormatter<'a>(&'a dyn std::error::Error);

impl std::fmt::Display for ErrorMessageFormatter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut error = Some(self.0);
        while let Some(e) = error {
            write!(f, "{e}")?;
            error = e.source();
            if error.is_some() {
                write!(f, ": ")?;
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ErrorBacktraceFormatter<'a>(&'a dyn std::error::Error);

impl std::fmt::Display for ErrorBacktraceFormatter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Uses the innermost captured backtrace
        let mut error = Some(self.0);
        let mut backtrace = None;
        while let Some(e) = error {
            if let Some(bt) = core::error::request_ref::<Backtrace>(e) {
                if bt.status() == BacktraceStatus::Captured {
                    backtrace = Some(bt);
                }
            }
            error = e.source();
        }

        if let Some(backtrace) = backtrace {
            write!(f, "{backtrace}")
        } else {
            write!(f, "<unavailable>")
        }
    }
}
