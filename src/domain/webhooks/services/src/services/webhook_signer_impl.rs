// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose;
use chrono::{DateTime, Utc};
use kamu_webhooks::{
    WebhookRFC9421Headers,
    WebhookSigner,
    WebhookSubscriptionSecret,
    WebhooksConfig,
};
use secrecy::SecretString;

use crate::{HEADER_CONTENT_DIGEST, HEADER_WEBHOOK_TIMESTAMP, KAMU_WEBHOOK_KEY_ID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSignerImpl {
    webhook_secret_encryption_key: Option<SecretString>,
}

#[dill::component(pub)]
#[dill::interface(dyn WebhookSigner)]
impl WebhookSignerImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(webhooks_config: Arc<WebhooksConfig>) -> Self {
        Self {
            webhook_secret_encryption_key: webhooks_config
                .secret_encryption_key
                .as_ref()
                .map(|key| SecretString::from(key.clone())),
        }
    }

    fn canonicalize_field(
        field: &SigningField,
        headers: &http::HeaderMap,
        method: &http::Method,
        uri: &http::Uri,
    ) -> Option<(String, String)> {
        match field {
            SigningField::Header(name) => {
                let name_lc = name.to_ascii_lowercase();
                headers.get(&name_lc).map(|v| {
                    let val = v.to_str().unwrap_or("").trim();
                    (format!("\"{name_lc}\"",), format!(": {val}",))
                })
            }
            SigningField::Derived(s) if *s == SIGNING_FIELD_METHOD => {
                Some(((*s).to_string(), format!(": {}", method.as_str())))
            }
            SigningField::Derived(s) if *s == SIGNING_FIELD_PATH => {
                Some(((*s).to_string(), format!(": {}", uri.path())))
            }
            SigningField::Derived(s) if *s == SIGNING_FIELD_AUTHORITY => uri
                .authority()
                .map(|a| ((*s).to_string(), format!(": {}", a.as_str()))),
            _ => None,
        }
    }

    fn build_signature_input(sig_input: &SignatureInput) -> String {
        let header_list: Vec<String> = sig_input
            .headers
            .iter()
            .map(|h| match h {
                SigningField::Header(hn) => format!("\"{}\"", hn.to_ascii_lowercase()),
                SigningField::Derived(hn) => (*hn).to_string(),
            })
            .collect();

        let params = [
            format!("keyid=\"{}\"", sig_input.key_id),
            format!("alg=\"{}\"", sig_input.algorithm),
            format!("created={}", sig_input.created),
        ];

        format!("sig1=({}); {}", header_list.join(" "), params.join("; "))
    }

    fn build_signature_base(
        sig_input: &SignatureInput,
        headers: &http::HeaderMap,
        method: &http::Method,
        uri: &http::Uri,
    ) -> String {
        let mut lines = Vec::new();

        for field in &sig_input.headers {
            if let Some((key, value)) = Self::canonicalize_field(field, headers, method, uri) {
                lines.push(format!("{key}{value}"));
            }
        }

        lines.push(format!(
            "\"@signature-params\": ({})",
            sig_input
                .headers
                .iter()
                .map(|f| match f {
                    SigningField::Header(h) => format!("\"{}\"", h.to_ascii_lowercase()),
                    SigningField::Derived(h) => (*h).to_string(),
                })
                .collect::<Vec<_>>()
                .join(" ")
        ));

        lines.push(format!(
            ";keyid=\"{}\";alg=\"{}\";created={}",
            sig_input.key_id, sig_input.algorithm, sig_input.created
        ));

        lines.join("\n")
    }

    fn create_signature(base: &str, secret: &[u8]) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
        let tag = ring::hmac::sign(&key, base.as_bytes());
        general_purpose::STANDARD.encode(tag)
    }

    fn compute_content_digest(payload: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(payload);
        let digest = hasher.finalize();

        let b64 = general_purpose::STANDARD.encode(digest);

        format!("sha-256=:{b64}:")
    }

    fn generate_rfc9421_headers_impl(
        secret: &[u8],
        method: &http::Method,
        uri: &http::Uri,
        payload_bytes: &[u8],
        webhook_timestamp: i64,
    ) -> WebhookRFC9421Headers {
        let mut headers = http::HeaderMap::new();

        let digest_value = Self::compute_content_digest(payload_bytes);
        headers.insert(HEADER_CONTENT_DIGEST, digest_value.parse().unwrap());

        headers.insert(
            HEADER_WEBHOOK_TIMESTAMP,
            webhook_timestamp.to_string().parse().unwrap(),
        );

        let sig_input = SignatureInput {
            key_id: String::from(KAMU_WEBHOOK_KEY_ID),
            algorithm: "hmac-sha256",
            headers: vec![
                SigningField::Derived(SIGNING_FIELD_METHOD),
                SigningField::Derived(SIGNING_FIELD_PATH),
                SigningField::Derived(SIGNING_FIELD_AUTHORITY),
                SigningField::Header(HEADER_WEBHOOK_TIMESTAMP.into()),
                SigningField::Header(HEADER_CONTENT_DIGEST.into()),
            ],
            created: webhook_timestamp,
        };

        let signature_input_header = Self::build_signature_input(&sig_input);
        let base_string = Self::build_signature_base(&sig_input, &headers, method, uri);

        let signature = Self::create_signature(&base_string, secret);
        let signature_header = format!("sig1=:{signature}:");

        WebhookRFC9421Headers {
            signature: signature_header,
            signature_input: signature_input_header,
            content_digest: digest_value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookSigner for WebhookSignerImpl {
    fn generate_rfc9421_headers(
        &self,
        secret: &WebhookSubscriptionSecret,
        webhook_created_at: DateTime<Utc>,
        payload_bytes: &[u8],
        target_url: &url::Url,
    ) -> WebhookRFC9421Headers {
        let uri = target_url
            .as_str()
            .parse::<http::Uri>()
            .expect("Failed to parse URL");

        let secret = secret
            .get_exposed_value(self.webhook_secret_encryption_key.as_ref())
            .unwrap();

        Self::generate_rfc9421_headers_impl(
            secret.as_slice(),
            &http::Method::POST,
            &uri,
            payload_bytes,
            webhook_created_at.timestamp(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a field to be signed
#[derive(Debug, Clone)]
enum SigningField {
    Header(String),
    Derived(&'static str),
}

const SIGNING_FIELD_AUTHORITY: &str = "@authority";
const SIGNING_FIELD_METHOD: &str = "@method";
const SIGNING_FIELD_PATH: &str = "@path";

struct SignatureInput {
    pub key_id: String,
    pub algorithm: &'static str,
    pub headers: Vec<SigningField>, // Ordered
    pub created: i64,               // Unix timestamp in seconds
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
