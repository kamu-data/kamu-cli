// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretExt, SecretSetResource, SecretSetSpec, SecretSetSpecInput};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ResourceSchemaProvider,
};
use kamu_resources_services::ResourceDispatcherFactory;
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_spec_view_dispatcher_reveals_encrypted_secrets_as_plaintext() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
        secrets: odf::metadata::config::Secrets {
            entries: [
                (
                    "API_TOKEN".to_string(),
                    odf::metadata::config::Secret {
                        value: "my-secret-token".to_string(),
                        content_encoding: None,
                    },
                ),
                (
                    "DB_PASSWORD".to_string(),
                    odf::metadata::config::Secret {
                        value: "my-db-password".to_string(),
                        content_encoding: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        },
    });

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_handle, "test-secrets"),
            spec,
        })
        .await
        .unwrap();

    let applied_id = match decision {
        ApplyResourceApplicationDecision::Applied(result) => result.id,
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("apply was rejected: {}", r.message)
        }
    };

    let snapshot = harness
        .generic_query_svc()
        .get_snapshot_by_id(&applied_id)
        .await
        .unwrap()
        .expect("snapshot must exist after apply");

    // Confirm stored form has encrypted secrets (precondition)
    let stored_spec: SecretSetSpec =
        serde_json::from_value(snapshot.spec.clone()).expect("spec must deserialize");
    for (name, secret) in &stored_spec.secrets.entries {
        assert!(
            secret.is_encrypted(),
            "precondition: '{name}' must be encrypted in stored spec"
        );
    }

    // Resolve the view dispatcher from catalog and call reveal_spec
    let dispatcher = harness
        .catalog()
        .get_one::<ResourceDispatcherFactory>()
        .unwrap()
        .spec_view_dispatcher(SecretSetResource::schema())
        .expect("SecretSetSpecViewDispatcher must be registered");

    let revealed_json = dispatcher
        .reveal_spec(snapshot.spec)
        .expect("reveal_spec must succeed");

    let revealed_spec: SecretSetSpec =
        serde_json::from_value(revealed_json).expect("revealed spec must deserialize");

    // After reveal, all secrets must carry the original plaintext with no
    // contentEncoding
    assert_eq!(
        revealed_spec.secrets.entries["API_TOKEN"],
        odf::metadata::config::Secret {
            value: "my-secret-token".to_string(),
            content_encoding: None,
        },
        "API_TOKEN must be revealed as its original plaintext"
    );
    assert_eq!(
        revealed_spec.secrets.entries["DB_PASSWORD"],
        odf::metadata::config::Secret {
            value: "my-db-password".to_string(),
            content_encoding: None,
        },
        "DB_PASSWORD must be revealed as its original plaintext"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
