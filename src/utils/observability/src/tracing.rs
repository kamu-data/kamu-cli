// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wrapper around the stock exporter that filters out `trace_id` attributes
/// from spans that we add to have `trace_id` appear in logs. We don't want them
/// to appear in OTEL data as they cause `Duplicated tag` warnings.
#[cfg(feature = "opentelemetry")]
#[derive(Debug)]
pub struct FilteringExporter<E>(pub E);

#[cfg(feature = "opentelemetry")]
impl<E: opentelemetry_sdk::trace::SpanExporter> opentelemetry_sdk::trace::SpanExporter
    for FilteringExporter<E>
{
    fn export(
        &self,
        mut batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> impl Future<Output = opentelemetry_sdk::error::OTelSdkResult> {
        for span in &mut batch {
            span.attributes.retain(|kv| kv.key.as_str() != "trace_id");
        }
        self.0.export(batch)
    }

    fn shutdown_with_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn shutdown(&mut self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown()
    }

    fn force_flush(&mut self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.0.set_resource(resource)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
