// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Config {
    /// Mode to use - auto-detects if not specified
    pub mode: Option<Mode>,
    /// Name of the service that will appear e.g. in OTEL traces
    pub service_name: String,
    /// Version of the service that will appear e.g. in OTEL traces
    pub service_version: String,
    /// Log levels that will be used if `RUST_LOG` was not specified explicitly
    pub default_log_levels: String,
    /// OpenTelemetry protocol endpoint to export traces to
    pub otlp_endpoint: Option<String>,
    /// Tracing span limits
    pub span_limits: SpanLimits,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: None,
            service_name: "unnamed-service".to_string(),
            service_version: "0.0.0".to_string(),
            default_log_levels: "info".to_string(),
            otlp_endpoint: None,
            span_limits: SpanLimits::default(),
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        Self::from_env_with_prefix("")
    }

    pub fn from_env_with_prefix(prefix: &str) -> Self {
        // TODO: Use configuration crate like confique to avoid boilerplate?
        let mut cfg = Self::default();

        if let Some(mode) = std::env::var(format!("{prefix}MODE"))
            .ok()
            .map(|s| s.to_lowercase())
            .filter(|v| !v.is_empty())
        {
            cfg.mode = match mode.to_ascii_lowercase().as_ref() {
                "dev" => Some(Mode::Dev),
                "service" => Some(Mode::Service),
                _ => None,
            };
        }
        if let Some(service_name) = std::env::var(format!("{prefix}SERVICE_NAME"))
            .ok()
            .filter(|v| !v.is_empty())
        {
            cfg.service_name = service_name;
        }
        if let Some(service_version) = std::env::var(format!("{prefix}SERVICE_VERSION"))
            .ok()
            .filter(|v| !v.is_empty())
        {
            cfg.service_version = service_version;
        }
        if let Some(otlp_endpoint) = std::env::var(format!("{prefix}OTLP_ENDPOINT"))
            .ok()
            .filter(|v| !v.is_empty())
        {
            cfg.otlp_endpoint = Some(otlp_endpoint);
        }
        if let Some(value) = std::env::var(format!("{prefix}MAX_EVENTS_PER_SPAN"))
            .ok()
            .and_then(|v| str::parse(&v).ok())
        {
            cfg.span_limits.max_events_per_span = value;
        }
        if let Some(value) = std::env::var(format!("{prefix}MAX_ATTRIBUTES_PER_SPAN"))
            .ok()
            .and_then(|v| str::parse(&v).ok())
        {
            cfg.span_limits.max_attributes_per_span = value;
        }
        if let Some(value) = std::env::var(format!("{prefix}MAX_LINKS_PER_SPAN"))
            .ok()
            .and_then(|v| str::parse(&v).ok())
        {
            cfg.span_limits.max_links_per_span = value;
        }
        if let Some(value) = std::env::var(format!("{prefix}MAX_ATTRIBUTES_PER_EVENT"))
            .ok()
            .and_then(|v| str::parse(&v).ok())
        {
            cfg.span_limits.max_attributes_per_event = value;
        }
        if let Some(value) = std::env::var(format!("{prefix}MAX_ATTRIBUTES_PER_LINK"))
            .ok()
            .and_then(|v| str::parse(&v).ok())
        {
            cfg.span_limits.max_attributes_per_link = value;
        }
        cfg
    }

    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = service_name.into();
        self
    }

    pub fn with_service_version(mut self, service_version: impl Into<String>) -> Self {
        self.service_version = service_version.into();
        self
    }

    pub fn with_default_log_levels(mut self, default_log_levels: impl Into<String>) -> Self {
        self.default_log_levels = default_log_levels.into();
        self
    }

    pub fn with_otlp_endpoint(mut self, otlp_endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(otlp_endpoint.into());
        self
    }

    pub fn with_max_events_per_span(mut self, value: u32) -> Self {
        self.span_limits.max_events_per_span = value;
        self
    }

    pub fn with_max_attributes_per_span(mut self, value: u32) -> Self {
        self.span_limits.max_attributes_per_span = value;
        self
    }

    pub fn with_max_links_per_span(mut self, value: u32) -> Self {
        self.span_limits.max_links_per_span = value;
        self
    }

    pub fn with_max_attributes_per_event(mut self, value: u32) -> Self {
        self.span_limits.max_attributes_per_event = value;
        self
    }

    pub fn with_max_attributes_per_link(mut self, value: u32) -> Self {
        self.span_limits.max_attributes_per_link = value;
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Allipation is running in development mode with human-readable
    /// well-formatted output for traces and errors directed to STDERR
    Dev,
    /// Application is running in a service mode (e.g. deployed in kubernetes)
    /// with machine-readable JSON output directed to STDERR
    Service,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct SpanLimits {
    /// The max events that can be added to a `Span`.
    pub max_events_per_span: u32,
    /// The max attributes that can be added to a `Span`.
    pub max_attributes_per_span: u32,
    /// The max links that can be added to a `Span`.
    pub max_links_per_span: u32,
    /// The max attributes that can be added into an `Event`
    pub max_attributes_per_event: u32,
    /// The max attributes that can be added into a `Link`
    pub max_attributes_per_link: u32,
}

impl Default for SpanLimits {
    fn default() -> Self {
        SpanLimits {
            max_events_per_span: 128,
            max_attributes_per_span: 128,
            max_links_per_span: 128,
            max_attributes_per_link: 128,
            max_attributes_per_event: 128,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
