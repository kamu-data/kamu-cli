// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Verifies a received webhook the way a customer's receiver does: it trusts
//! nothing but the request, its own public URL and the subscription secret,
//! and rebuilds the signature base from what `Signature-Input` declares.

use std::fmt::Write as _;
use std::sync::LazyLock;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use regex::Regex;
use sha2::Digest as _;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SIGNATURE_LABEL: &str = "sig1";

/// What a successfully verified signature covered
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VerifiedWebhookSignature {
    /// Signed components, in the declared order: derived ones like `@method`
    /// and lowercase header names
    pub covered_components: Vec<String>,
    /// Signature parameters, in the declared order, values unquoted
    pub params: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WebhookVerificationError {
    MissingHeader(&'static str),
    Malformed(&'static str),
    UnsupportedAlgorithm(String),
    UnsupportedComponent(String),
    /// A header listed in `Signature-Input` is not in the request
    MissingSignedHeader(String),
    /// The body does not hash to `Content-Digest`
    DigestMismatch,
    /// The HMAC of the signature base does not match `Signature`
    SignatureMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks the body against `Content-Digest`, then the HMAC-SHA256 signature
/// keyed with the secret's bytes. `public_url` is the receiver's own URL, as
/// registered in the subscription.
pub(crate) fn verify_webhook(
    request: &wiremock::Request,
    public_url: &Url,
    secret: &str,
) -> Result<VerifiedWebhookSignature, WebhookVerificationError> {
    verify_content_digest(&request.body, header(request, "content-digest")?)?;

    let SignatureInput {
        covered_components,
        raw_params,
    } = parse_signature_input(header(request, "signature-input")?)?;
    let signature = parse_signature(header(request, "signature")?)?;

    let params: Vec<(String, String)> = raw_params
        .iter()
        .map(|(key, raw_value)| (key.clone(), raw_value.trim_matches('"').to_string()))
        .collect();
    match params.iter().find(|(key, _)| key == "alg") {
        Some((_, alg)) if alg == "hmac-sha256" => {}
        Some((_, alg)) => return Err(WebhookVerificationError::UnsupportedAlgorithm(alg.clone())),
        None => {
            return Err(WebhookVerificationError::Malformed(
                "alg parameter is missing",
            ));
        }
    }

    let mut base_lines = covered_components
        .iter()
        .map(|component| {
            signature_base_line(request, public_url, component)
                .map(|value| format!("{}: {value}", quote_component(component)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    base_lines.push(format!(
        "\"@signature-params\": ({})",
        covered_components
            .iter()
            .map(|component| quote_component(component))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    let mut params_line = String::new();
    for (key, raw_value) in &raw_params {
        write!(params_line, ";{key}={raw_value}").unwrap();
    }
    base_lines.push(params_line);

    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    ring::hmac::verify(&key, base_lines.join("\n").as_bytes(), &signature)
        .map_err(|_| WebhookVerificationError::SignatureMismatch)?;

    Ok(VerifiedWebhookSignature {
        covered_components,
        params,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// RFC 9530 `Content-Digest`, `sha-256` only
fn verify_content_digest(body: &[u8], header_value: &str) -> Result<(), WebhookVerificationError> {
    static CONTENT_DIGEST: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^([A-Za-z0-9_-]+)=:([^:]+):$").unwrap());

    let captures = CONTENT_DIGEST
        .captures(header_value)
        .ok_or(WebhookVerificationError::Malformed("Content-Digest"))?;
    if !captures[1].eq_ignore_ascii_case("sha-256") {
        return Err(WebhookVerificationError::UnsupportedAlgorithm(
            captures[1].to_string(),
        ));
    }
    let expected_digest = BASE64
        .decode(&captures[2])
        .map_err(|_| WebhookVerificationError::Malformed("Content-Digest"))?;

    if sha2::Sha256::digest(body).as_slice() == expected_digest.as_slice() {
        Ok(())
    } else {
        Err(WebhookVerificationError::DigestMismatch)
    }
}

/// The `sig1` entry of `Signature-Input`
struct SignatureInput {
    covered_components: Vec<String>,
    /// Parameters in the declared order, values as written (quoted or not)
    raw_params: Vec<(String, String)>,
}

/// `sig1=(<components>); key=value; ...`
fn parse_signature_input(header_value: &str) -> Result<SignatureInput, WebhookVerificationError> {
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

    Ok(SignatureInput {
        covered_components,
        raw_params,
    })
}

/// `sig1=:<base64>:` into the signature bytes
fn parse_signature(header_value: &str) -> Result<Vec<u8>, WebhookVerificationError> {
    static SIGNATURE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\w+)=:([^:]+):").unwrap());

    let captures = SIGNATURE
        .captures_iter(header_value)
        .find(|captures| &captures[1] == SIGNATURE_LABEL)
        .ok_or(WebhookVerificationError::Malformed("Signature"))?;

    BASE64
        .decode(&captures[2])
        .map_err(|_| WebhookVerificationError::Malformed("Signature"))
}

/// Value of one covered component: derived ones come from the request line and
/// the receiver's public URL, the rest from the request headers
fn signature_base_line(
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
        derived if derived.starts_with('@') => Err(WebhookVerificationError::UnsupportedComponent(
            derived.to_string(),
        )),
        header_name => request
            .headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim().to_string())
            .ok_or_else(|| WebhookVerificationError::MissingSignedHeader(header_name.to_string())),
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

fn header<'a>(
    request: &'a wiremock::Request,
    name: &'static str,
) -> Result<&'a str, WebhookVerificationError> {
    request
        .headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or(WebhookVerificationError::MissingHeader(name))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderMap;

    use super::*;

    /// A real delivery and its secret, from the TypeScript verification
    /// example given to customers, which accepts it
    fn customer_example_delivery() -> (wiremock::Request, Url, &'static str) {
        let headers: HeaderMap = [
            ("content-type", "application/json"),
            ("user-agent", "Kamu-Webhook/1.0"),
            ("x-webhook-delivery-attempt", "1"),
            ("x-webhook-event-type", "DATASET.REF.UPDATED"),
            ("x-webhook-subscription-id", "99dbf74d-e950-45c7-89dd-542e22f16be4"),
            ("x-webhook-event-id", "f25b9c8c-c43c-4e27-886d-0f339558dc42"),
            ("x-webhook-timestamp", "1747736262"),
            (
                "signature-input",
                r#"sig1=(@method @path @authority "x-webhook-timestamp" "content-digest"); keyid="default"; alg="hmac-sha256"; created=1747736262"#,
            ),
            ("signature", "sig1=:YEF5hEQjreSeIWmM1l85WxCrbQw6wqJFog14gD/nSMw=:"),
            (
                "content-digest",
                "sha-256=:FgNJxPsdFxi5kmngHC50s1KlfbCl6mwGGnWUziIdhGo=:",
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.parse().unwrap(), value.parse().unwrap()))
        .collect();

        let body = concat!(
            r#"{"blockRef":"head","#,
            r#""datasetId":"did:odf:fed011ba584ba51fc73fa59c8f34b6bedfb8bb7222fe5b4bfe218629e67a32f8afcf8","#,
            r#""newHash":"f1620b8e49275829891495332c7449f74f4273b2a6df521bf7d8e9de60b81341c0d79","#,
            r#""oldHash":"f16206fbee177eae356bdf5d529882ad02d2d6696df9020228f33f160a98919f1df85","#,
            r#""ownerAccountId":"did:odf:fed01b262b4f0f0573f9ed7497c820b7be6dcc62a285d9af49c86e3ad10eea131a462","#,
            r#""version":1}"#,
        );

        let public_url =
            Url::parse("https://webhook.site/b353da00-c73b-4436-a367-a8fbfe4ec800").unwrap();

        let request = wiremock::Request {
            url: public_url.clone(),
            method: reqwest::Method::POST,
            headers,
            body: body.as_bytes().to_vec(),
        };

        (
            request,
            public_url,
            "12874abef1b0fd1caf25705d88cb753cc5057b364f818d8626716cfe19f6424b",
        )
    }

    #[test]
    fn test_webhook_receiver_accepts_customer_example() {
        let (request, public_url, secret) = customer_example_delivery();

        assert_eq!(
            verify_webhook(&request, &public_url, secret)
                .map(|verified| verified.covered_components),
            Ok([
                "@method",
                "@path",
                "@authority",
                "x-webhook-timestamp",
                "content-digest"
            ]
            .map(ToString::to_string)
            .to_vec())
        );
    }

    #[test]
    fn test_webhook_receiver_rejects_customer_example_with_another_secret() {
        let (request, public_url, _) = customer_example_delivery();

        assert_eq!(
            verify_webhook(&request, &public_url, "not-the-secret"),
            Err(WebhookVerificationError::SignatureMismatch)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
