// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Config {
    /// Name of the service that will appear e.g. in OTEL traces
    pub service_name: String,
    /// Version of the service that will appear e.g. in OTEL traces
    pub service_version: String,
    /// Log levels that will be used if `RUST_LOG` was not specified explicitly
    pub default_log_levels: String,
    /// OpenTelemetry protocol endpoint to export traces to
    pub otlp_endpoint: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            service_name: "unnamed-service".to_string(),
            service_version: "0.0.0".to_string(),
            default_log_levels: "info".to_string(),
            otlp_endpoint: None,
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        Self::from_env_with_prefix("")
    }

    pub fn from_env_with_prefix(prefix: &str) -> Self {
        let mut cfg = Self::default();
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
