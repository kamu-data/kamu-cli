// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use http::{Response, StatusCode, Uri, header};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn http_layer() -> HttpTraceLayer {
    HttpTraceLayer
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct HttpTraceLayer;

impl<S> tower::Layer<S> for HttpTraceLayer {
    type Service = HttpTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpTraceService { inner }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct HttpTraceService<S> {
    inner: S,
}

impl<S, B> tower::Service<http::Request<B>> for HttpTraceService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
    <S as tower::Service<http::Request<B>>>::Error: std::error::Error,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = HttpTraceFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let span = {
            let method = request.method();
            let route = RouteOrUri::from(&request);

            let span = crate::tracing::root_span!(
                "Http::request",
                method = %method,
                route = %route,
                "otel.name" = tracing::field::Empty,
            );

            #[cfg(feature = "opentelemetry")]
            {
                crate::tracing::include_otel_trace_id(&span);
                span.record("otel.name", format!("{method} {route}"));
            }

            {
                let _enter = span.enter();
                tracing::info!(
                    uri = %request.uri(),
                    version = ?request.version(),
                    headers = ?request.headers(),
                    "HTTP request",
                );
            }

            span
        };

        let future = self.inner.call(request);
        let start = Instant::now();

        HttpTraceFuture {
            inner: future,
            span,
            start,
            done: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pin_project_lite::pin_project! {
    pub struct HttpTraceFuture<F> {
        #[pin]
        inner: F,
        span: tracing::Span,
        start: Instant,
        // Set to true once we've logged the response, to suppress the drop guard.
        done: bool,
    }

    impl<F> PinnedDrop for HttpTraceFuture<F> {
        fn drop(this: Pin<&mut Self>) {
            if !this.done {
                let _enter = this.span.enter();
                let processing_time = this.start.elapsed();
                tracing::warn!(
                    processing_time = %DurationInMillis(processing_time),
                    "HTTP abort",
                );
            }
        }
    }
}

impl<F, E> Future for HttpTraceFuture<F>
where
    F: Future<Output = Result<http::Response<Body>, E>>,
    E: std::error::Error,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _enter = this.span.enter();

        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                *this.done = true;
                let processing_time = this.start.elapsed();
                match &result {
                    Ok(resp) if !resp.status().is_server_error() => tracing::info!(
                        status = resp.status().as_u16(),
                        headers = ?resp.headers(),
                        processing_time = %DurationInMillis(processing_time),
                        "HTTP response",
                    ),
                    Ok(resp) => tracing::error!(
                        status = resp.status().as_u16(),
                        headers = ?resp.headers(),
                        processing_time = %DurationInMillis(processing_time),
                        "HTTP response",
                    ),
                    Err(err) => tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "HTTP unhandled error",
                    ),
                }
                Poll::Ready(result)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DurationInMillis(std::time::Duration);

impl std::fmt::Display for DurationInMillis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ms", self.0.as_millis())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum RouteOrUri<'a> {
    Route(&'a str),
    Uri(&'a Uri),
}

impl<'a, B> From<&'a http::Request<B>> for RouteOrUri<'a> {
    fn from(request: &'a http::Request<B>) -> Self {
        request
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map_or_else(
                || RouteOrUri::Uri(request.uri()),
                |m| RouteOrUri::Route(m.as_str()),
            )
    }
}

impl std::fmt::Display for RouteOrUri<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteOrUri::Route(s) => write!(f, "{s}"),
            RouteOrUri::Uri(uri) => write!(f, "{uri}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
pub async fn unknown_fallback_handler(
    request: axum::http::Request<axum::body::Body>,
) -> impl axum::response::IntoResponse {
    tracing::warn!(
        method = %request.method(),
        uri = %request.uri(),
        version = ?request.version(),
        headers = ?request.headers(),
        "HTTP: fallback request",
    );
    (axum::http::StatusCode::NOT_FOUND, "Not Found")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn panic_handler(_err: Box<dyn Any + Send + 'static>) -> Response<Body> {
    let body = Body::from(r#"{"error":"Internal Server Error"}"#);
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}
