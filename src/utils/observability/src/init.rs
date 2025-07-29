// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use super::config::Config;
use crate::config::Mode;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uses heuristics to pick the right mode
pub fn auto(cfg: Config) -> Guard {
    match cfg.mode.unwrap_or_else(auto_detect_mode) {
        Mode::Dev => dev(cfg),
        Mode::Service => service(cfg),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// If application is started under a terminal will use the [`dev`] mode,
/// otherwise will use [`service`] mode. Mode can be overridden via config.
pub fn auto_detect_mode() -> Mode {
    if std::io::stderr().is_terminal() {
        Mode::Dev
    } else {
        Mode::Service
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn dev(cfg: Config) -> Guard {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(cfg.default_log_levels.clone()));

    let text_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(std::io::stderr)
        .with_line_number(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    #[cfg(feature = "opentelemetry")]
    let (otel_layer, otlp_guard) = if cfg.otlp_endpoint.is_none() {
        (None, None)
    } else {
        (
            Some(
                tracing_opentelemetry::layer()
                    .with_error_records_to_exceptions(true)
                    .with_tracer(init_otel_tracer(&cfg)),
            ),
            Some(OtlpGuard),
        )
    };

    let reg = tracing_subscriber::registry().with(env_filter);
    #[cfg(feature = "opentelemetry")]
    let reg = reg.with(otel_layer);
    #[cfg(feature = "tracing-error")]
    let reg = reg.with(tracing_error::ErrorLayer::default());
    let reg = reg.with(text_layer);
    reg.init();

    Guard {
        non_blocking_appender: None,

        #[cfg(feature = "opentelemetry")]
        otlp_guard,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn service(cfg: Config) -> Guard {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new(cfg.default_log_levels.clone()));

    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    let text_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_line_number(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);

    #[cfg(feature = "opentelemetry")]
    let (otel_layer, otlp_guard) = if cfg.otlp_endpoint.is_none() {
        (None, None)
    } else {
        (
            Some(
                tracing_opentelemetry::layer()
                    .with_error_records_to_exceptions(true)
                    .with_tracer(init_otel_tracer(&cfg)),
            ),
            Some(OtlpGuard),
        )
    };

    let reg = tracing_subscriber::registry().with(env_filter);
    #[cfg(feature = "opentelemetry")]
    let reg = reg.with(otel_layer);
    #[cfg(feature = "tracing-error")]
    let reg = reg.with(tracing_error::ErrorLayer::default());
    let reg = reg.with(text_layer);
    reg.init();

    Guard {
        non_blocking_appender: Some(guard),

        #[cfg(feature = "opentelemetry")]
        otlp_guard,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "opentelemetry")]
fn init_otel_tracer(cfg: &Config) -> opentelemetry_sdk::trace::Tracer {
    use std::time::Duration;

    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_semantic_conventions::resource as otel_resource;

    let otel_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(cfg.otlp_endpoint.as_ref().unwrap())
        .with_timeout(Duration::from_secs(5));

    use opentelemetry::trace::TracerProvider;
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otel_exporter)
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                .with_max_events_per_span(cfg.span_limits.max_events_per_span)
                .with_max_attributes_per_span(cfg.span_limits.max_attributes_per_span)
                .with_max_links_per_span(cfg.span_limits.max_links_per_span)
                .with_max_attributes_per_event(cfg.span_limits.max_attributes_per_event)
                .with_max_attributes_per_link(cfg.span_limits.max_attributes_per_link)
                .with_resource(opentelemetry_sdk::Resource::new([
                    KeyValue::new(otel_resource::SERVICE_NAME, cfg.service_name.clone()),
                    KeyValue::new(otel_resource::SERVICE_VERSION, cfg.service_version.clone()),
                ]))
                .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Creating tracer provider")
        .tracer_builder(cfg.service_name.clone())
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[must_use]
#[allow(dead_code)]
pub struct Guard {
    pub non_blocking_appender: Option<tracing_appender::non_blocking::WorkerGuard>,

    #[cfg(feature = "opentelemetry")]
    pub otlp_guard: Option<OtlpGuard>,
}

pub struct OtlpGuard;

#[cfg(feature = "opentelemetry")]
impl Drop for OtlpGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}
