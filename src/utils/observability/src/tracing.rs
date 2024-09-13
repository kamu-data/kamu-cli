// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "opentelemetry")]
#[macro_export]
macro_rules! root_span {
    ($name:expr) => {
        $crate::tracing::root_span!($name,)
    };
    ($name:expr, $($field:tt)*) => {
        {
            let span = ::tracing::info_span!(
                $name,
                trace_id = tracing::field::Empty,
                $($field)*
            );

            $crate::tracing::include_otel_trace_id(&span);

            span
        }
    };
}

#[cfg(not(feature = "opentelemetry"))]
#[macro_export]
macro_rules! root_span {
    ($name:expr) => {
        $crate::tracing::root_span!($name,)
    };
    ($name:expr, $($field:tt)*) => {
        tracing::info_span!(
            $name,
            trace_id = tracing::field::Empty,
            $($field)*
        )
    };
}

pub use root_span;

/// Extracts trace ID from the OTEL layer and adds it to the tracing span to
/// allow cross-linking between logs and traces
#[cfg(feature = "opentelemetry")]
pub fn include_otel_trace_id(span: &tracing::Span) {
    use opentelemetry::trace::TraceContextExt as _;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    let context = span.context();
    let otel_span = context.span();
    let span_context = otel_span.span_context();
    let trace_id = span_context.trace_id();

    if span_context.is_valid() {
        span.record("trace_id", tracing::field::display(trace_id));
    }
}
