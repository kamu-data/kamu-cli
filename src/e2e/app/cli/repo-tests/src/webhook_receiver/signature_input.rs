// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write as _;
use std::sync::LazyLock;

use regex::Regex;
use url::Url;

use super::{SIGNATURE_LABEL, WebhookVerificationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `sig1` entry of the `Signature-Input` header:
/// `sig1=(<components>); key=value; ...`
pub(super) struct SignatureInput {
    /// Signed components, in the declared order: derived ones like `@method`
    /// and lowercase header names
    pub covered_components: Vec<String>,
    /// Parameters in the declared order, values as written (quoted or not)
    raw_params: Vec<(String, String)>,
}

impl SignatureInput {
    pub fn parse(header_value: &str) -> Result<Self, WebhookVerificationError> {
        static SIGNATURE_INPUT: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(\w+)=\(([^)]*)\)([^,]*)").unwrap());
        static COMPONENT: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#""([^"]+)"|(@[^\s")]+)"#).unwrap());
        static PARAM: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r#";\s*(\w+)=("[^"]*"|[^\s;]+)"#).unwrap());

        let captures = SIGNATURE_INPUT
            .captures_iter(header_value)
            .find(|captures| &captures[1] == SIGNATURE_LABEL)
            .ok_or(WebhookVerificationError::Malformed("Signature-Input"))?;

        let covered_components = COMPONENT
            .captures_iter(&captures[2])
            .map(|component| {
                component
                    .get(1)
                    .or_else(|| component.get(2))
                    .unwrap()
                    .as_str()
                    .to_ascii_lowercase()
            })
            .collect();
        let raw_params = PARAM
            .captures_iter(&captures[3])
            .map(|param| (param[1].to_string(), param[2].to_string()))
            .collect();

        Ok(Self {
            covered_components,
            raw_params,
        })
    }

    /// Parameters in the declared order, values unquoted
    pub fn params(&self) -> Vec<(String, String)> {
        self.raw_params
            .iter()
            .map(|(key, raw_value)| (key.clone(), raw_value.trim_matches('"').to_string()))
            .collect()
    }

    /// The signed text: a line per covered component, the
    /// `@signature-params` line, and the parameters line
    pub fn signature_base(
        &self,
        request: &wiremock::Request,
        public_url: &Url,
    ) -> Result<String, WebhookVerificationError> {
        let mut base_lines = self
            .covered_components
            .iter()
            .map(|component| {
                Self::component_value(request, public_url, component)
                    .map(|value| format!("{}: {value}", Self::quote_component(component)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        base_lines.push(format!(
            "\"@signature-params\": ({})",
            self.covered_components
                .iter()
                .map(|component| Self::quote_component(component))
                .collect::<Vec<_>>()
                .join(" ")
        ));
        // Kamu puts the parameters on a line of their own, unlike RFC 9421
        let mut params_line = String::new();
        for (key, raw_value) in &self.raw_params {
            write!(params_line, ";{key}={raw_value}").unwrap();
        }
        base_lines.push(params_line);

        Ok(base_lines.join("\n"))
    }

    /// Value of one covered component: derived ones come from the request line
    /// and the receiver's public URL, the rest from the request headers
    fn component_value(
        request: &wiremock::Request,
        public_url: &Url,
        component: &str,
    ) -> Result<String, WebhookVerificationError> {
        match component {
            "@method" => Ok(request.method.as_str().to_ascii_uppercase()),
            "@path" => Ok(public_url.path().to_string()),
            "@query" => Ok(format!("?{}", public_url.query().unwrap_or(""))),
            "@authority" => {
                let host = public_url.host_str().unwrap();
                Ok(match public_url.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_string(),
                })
            }
            "@scheme" => Ok(public_url.scheme().to_string()),
            "@target-uri" => Ok(public_url.to_string()),
            derived if derived.starts_with('@') => Err(
                WebhookVerificationError::UnsupportedComponent(derived.to_string()),
            ),
            header_name => request
                .headers
                .get(header_name)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim().to_string())
                .ok_or_else(|| {
                    WebhookVerificationError::MissingSignedHeader(header_name.to_string())
                }),
        }
    }

    /// Derived components are written bare, header names in quotes
    fn quote_component(component: &str) -> String {
        if component.starts_with('@') {
            component.to_string()
        } else {
            format!("\"{component}\"")
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
