// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_webhooks::{WebhookSigner, WebhookSubscriptionSecret, WebhooksConfig};
use kamu_webhooks_services::WebhookSignerImpl;
use secrecy::SecretString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sign_dataset_ref_updated_webhook_with_unencrypted_secret() {
    let webhook_signer = WebhookSignerImpl::new(Arc::new(WebhooksConfig::default()));
    let webhook_secret =
        WebhookSubscriptionSecret::try_new(None, &SecretString::from("test_secret")).unwrap();

    test_sign_dataset_ref_updated_webhook(&webhook_signer, &webhook_secret);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sign_dataset_ref_updated_webhook_with_encrypted_secret() {
    let config = WebhooksConfig::sample();
    let webhook_signer = WebhookSignerImpl::new(Arc::new(config.clone()));
    let encryption_key = config
        .secret_encryption_key
        .as_ref()
        .map(|key| SecretString::from(key.clone()));
    let webhook_secret = WebhookSubscriptionSecret::try_new(
        encryption_key.as_ref(),
        &SecretString::from("test_secret"),
    )
    .unwrap();

    test_sign_dataset_ref_updated_webhook(&webhook_signer, &webhook_secret);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_sign_dataset_ref_updated_webhook(
    webhook_signer: &WebhookSignerImpl,
    webhook_secret: &WebhookSubscriptionSecret,
) {
    let payload = serde_json::json!({
      "version": "1",
      "datasetId": odf::DatasetID::new_seeded_ed25519(b"test_dataset_id").to_string(),
      "ownerAccountId": odf::AccountID::new_seeded_ed25519(b"test_account_id").to_string(),
      "blockRef": "head",
      "oldHash": odf::Multihash::from_digest_sha3_256(b"old_hash").to_string(),
      "newHash": odf::Multihash::from_digest_sha3_256(b"new_hash").to_string(),
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    let webhook_created_at = DateTime::parse_from_rfc3339("2025-05-13T22:56:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let headers = webhook_signer.generate_rfc9421_headers(
        webhook_secret,
        webhook_created_at,
        &payload_bytes,
        &url::Url::parse("https://example.com/webhook").unwrap(),
    );

    // Note: the expected values are computed at first run of the test
    // and then hardcoded here to avoid flakiness due to time-based values

    assert_eq!(
        headers.signature,
        "sig1=:coS90CuAAmr2mVsp2/k9rm8q2D8KCPT2uJmlKcYTgZE=:"
    );

    assert_eq!(
        headers.signature_input,
        "sig1=(@method @path @authority \"x-webhook-timestamp\" \"content-digest\"); \
         keyid=\"default\"; alg=\"hmac-sha256\"; created=1747176960"
    );

    assert_eq!(
        headers.content_digest,
        "sha-256=:BA2bN3rMDmEBxxu0hmTrkJjzPOVTtU1Co6fM1L+8tDg=:"
    );
}
