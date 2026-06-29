// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetResource, SecretSetSpec, SecretSpec, SecretValueSpec};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ResourceDescriptorProvider,
    get_resource_spec_view_dispatcher_from_catalog,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_spec_view_dispatcher_reveals_encrypted_secrets_as_plaintext() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let spec = SecretSetSpec {
        secrets: [
            (
                "API_TOKEN".to_string(),
                SecretSpec::Literal("my-secret-token".to_string()),
            ),
            (
                "DB_PASSWORD".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: "my-db-password".to_string(),
                }),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            uid: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, "test-secrets"),
            spec,
        })
        .await
        .unwrap();

    let applied_uid = match decision {
        ApplyResourceApplicationDecision::Applied(result) => result.uid,
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("apply was rejected: {}", r.message)
        }
    };

    let snapshot = harness
        .generic_query_svc()
        .get_snapshot_by_uid(&applied_uid)
        .await
        .unwrap()
        .expect("snapshot must exist after apply");

    // Confirm stored form has Encrypted variants (precondition)
    let stored_spec: SecretSetSpec =
        serde_json::from_value(snapshot.spec.clone()).expect("spec must deserialize");
    for (name, secret) in &stored_spec.secrets {
        assert!(
            secret.is_encrypted(),
            "precondition: '{name}' must be Encrypted in stored spec"
        );
    }

    // Resolve the view dispatcher from catalog and call reveal_spec
    let dispatcher = get_resource_spec_view_dispatcher_from_catalog(
        harness.catalog(),
        SecretSetResource::DESCRIPTOR.resource_type,
        SecretSetResource::DESCRIPTOR.api_version,
    )
    .expect("SecretSetSpecViewDispatcher must be registered");

    let revealed_json = dispatcher
        .reveal_spec(snapshot.spec)
        .expect("reveal_spec must succeed");

    let revealed_spec: SecretSetSpec =
        serde_json::from_value(revealed_json).expect("revealed spec must deserialize");

    // After reveal, all variants must be Literal with original plaintext
    assert_eq!(
        revealed_spec.secrets["API_TOKEN"],
        SecretSpec::Literal("my-secret-token".to_string()),
        "API_TOKEN must be revealed as its original plaintext"
    );
    assert_eq!(
        revealed_spec.secrets["DB_PASSWORD"],
        SecretSpec::Literal("my-db-password".to_string()),
        "DB_PASSWORD must be revealed as its original plaintext"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
