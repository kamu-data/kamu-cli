// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use reqwest::header::HeaderMap;
use url::Url;

use super::{WebhookReceiver, WebhookVerificationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_webhook_receiver_accepts_customer_example() {
    let example = CustomerExample::new();

    assert_eq!(
        WebhookReceiver::new(&example.public_url, example.secret)
            .verify(&example.request)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_webhook_receiver_rejects_customer_example_with_another_secret() {
    let example = CustomerExample::new();

    assert_eq!(
        WebhookReceiver::new(&example.public_url, "not-the-secret").verify(&example.request),
        Err(WebhookVerificationError::SignatureMismatch)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A real delivery and its secret, from the TypeScript verification example
/// given to customers, which accepts it
struct CustomerExample {
    request: wiremock::Request,
    public_url: Url,
    secret: &'static str,
}

impl CustomerExample {
    fn new() -> Self {
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

        Self {
            request: wiremock::Request {
                url: public_url.clone(),
                method: reqwest::Method::POST,
                headers,
                body: body.as_bytes().to_vec(),
            },
            public_url,
            secret: "12874abef1b0fd1caf25705d88cb753cc5057b364f818d8626716cfe19f6424b",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
