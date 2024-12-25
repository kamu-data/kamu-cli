// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn grpc_layer() -> tower_http::trace::TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    MakeSpan,
    OnRequest,
    OnResponse,
> {
    tower_http::trace::TraceLayer::new_for_http()
        .on_request(OnRequest)
        .on_response(OnResponse)
        .make_span_with(MakeSpan)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct OnRequest;

impl<B> tower_http::trace::OnRequest<B> for OnRequest {
    fn on_request(&mut self, request: &http::Request<B>, _: &tracing::Span) {
        tracing::info!(
            uri = %request.uri(),
            version = ?request.version(),
            headers = ?request.headers(),
            "GRPC request",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct OnResponse;

impl<B> tower_http::trace::OnResponse<B> for OnResponse {
    fn on_response(
        self,
        response: &http::Response<B>,
        latency: std::time::Duration,
        _span: &tracing::Span,
    ) {
        tracing::info!(
            status = response.status().as_u16(),
            headers = ?response.headers(),
            latency = %Latency(latency),
            "GRPC response"
        );
    }
}

struct Latency(std::time::Duration);

impl std::fmt::Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ms", self.0.as_millis())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct MakeSpan;

impl<B> tower_http::trace::MakeSpan<B> for MakeSpan {
    // TODO: Trace linking across requests
    fn make_span(&mut self, request: &http::Request<B>) -> tracing::Span {
        let path = request.uri().path();
        let mut parts = path.split('/').filter(|x| !x.is_empty());
        let service = parts.next().unwrap_or_default();
        let method = parts.next().unwrap_or_default();

        let span = crate::tracing::root_span!(
            "Grpc::request",
            %service,
            %method,
            "otel.name" = tracing::field::Empty,
        );

        #[cfg(feature = "opentelemetry")]
        {
            crate::tracing::include_otel_trace_id(&span);

            span.record("otel.name", format!("{service}::{method}"));
        }

        span
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
