// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use url::Url;

use super::{ContentDigest, Signature, SignatureInput, WebhookVerificationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A receiver of webhook deliveries, registered in a subscription
pub(crate) struct WebhookReceiver<'a> {
    /// The receiver's own URL, as registered in the subscription
    public_url: &'a Url,
    secret: &'a str,
}

impl<'a> WebhookReceiver<'a> {
    pub fn new(public_url: &'a Url, secret: &'a str) -> Self {
        Self { public_url, secret }
    }

    /// Checks the body against `Content-Digest`, then the HMAC-SHA256
    /// signature keyed with the secret's bytes
    pub fn verify(
        &self,
        request: &wiremock::Request,
    ) -> Result<VerifiedWebhookSignature, WebhookVerificationError> {
        ContentDigest::parse(Self::header(request, "content-digest")?)?.verify(&request.body)?;

        let signature_input = SignatureInput::parse(Self::header(request, "signature-input")?)?;
        let signature = Signature::parse(Self::header(request, "signature")?)?;

        let params = signature_input.params();
        match params.iter().find(|(key, _)| key == "alg") {
            Some((_, alg)) if alg == "hmac-sha256" => {}
            Some((_, alg)) => {
                return Err(WebhookVerificationError::UnsupportedAlgorithm(alg.clone()));
            }
            None => {
                return Err(WebhookVerificationError::Malformed(
                    "alg parameter is missing",
                ));
            }
        }

        let signature_base = signature_input.signature_base(request, self.public_url)?;
        signature.verify(self.secret, &signature_base)?;

        Ok(VerifiedWebhookSignature {
            covered_components: signature_input.covered_components,
            params,
        })
    }

    fn header<'r>(
        request: &'r wiremock::Request,
        name: &'static str,
    ) -> Result<&'r str, WebhookVerificationError> {
        request
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .ok_or(WebhookVerificationError::MissingHeader(name))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a successfully verified signature covered
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct VerifiedWebhookSignature {
    /// Signed components, in the declared order: derived ones like `@method`
    /// and lowercase header names
    pub covered_components: Vec<String>,
    /// Signature parameters, in the declared order, values unquoted
    pub params: Vec<(String, String)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
