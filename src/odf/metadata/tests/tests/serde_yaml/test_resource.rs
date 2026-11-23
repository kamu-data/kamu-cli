// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::BTreeMap;

use ::serde::Serialize;
use ::serde::de::DeserializeOwned;
use indoc::indoc;
use opendatafabric_metadata::auth::*;
use opendatafabric_metadata::config::*;
use opendatafabric_metadata::errors::ValidationError;
use opendatafabric_metadata::resources::*;
use opendatafabric_metadata::serde::yaml as serde;
use opendatafabric_metadata::serde::yaml::IntoDto;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_input_generics() {
    use ::serde::de::IgnoredAny as Ignore;

    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
        headers:
          name: my-secret
        spec:
          secrets:
            tls:
              value: <cert>
        "#
    );

    // Read only header ignoring the rest
    let val = read_resource::<Ignore>(data);

    pretty_assertions::assert_eq!(
        val,
        ResourceInput {
            schema: SecretSet::type_uri().cheap_clone(),
            headers: headers("my-secret", None),
            spec: Ignore,
        }
    );

    // Read & write typed spec
    let val = read_resource::<serde::config::SecretSetSpecInput>(data);

    pretty_assertions::assert_eq!(
        val,
        ResourceInput {
            schema: SecretSet::type_uri().cheap_clone(),
            headers: headers("my-secret", None),
            spec: SecretSetSpecInput {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                }
            },
        }
    );

    pretty_assertions::assert_eq!(
        write_resource::<serde::config::SecretSetSpecInput, _>(val),
        data
    );

    // Read & write generic spec
    let val = read_resource::<serde_json::Value>(data);

    pretty_assertions::assert_eq!(
        val,
        ResourceInput {
            schema: SecretSet::type_uri().cheap_clone(),
            headers: headers("my-secret", None),
            spec: json!({
                "secrets": {
                    "tls": {
                        "value": "<cert>",
                    }
                }
            })
        }
    );

    pretty_assertions::assert_eq!(write_resource::<serde_json::Value, _>(val), data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_input_short_forms() {
    #[serde_with::serde_as]
    #[derive(Debug, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
    struct MySpec {
        #[serde_as(as = "serde::config::Secrets")]
        secrets: Secrets,
        #[serde_as(as = "serde::config::Variables")]
        variables: Variables,
    }

    impl IntoDto for MySpec {
        type Dto = Self;
        fn into_dto(self) -> Result<Self::Dto, ValidationError> {
            Ok(self)
        }
    }

    let data = indoc!(
        r#"
        $schema: https://kamu.dev/schemas/core/v1/MyResource
        headers:
          name: my-thing
          account: sergiimk
        spec:
          secrets:
            tls: <cert>
          variables:
            poolSize: '10'
        "#
    );

    let val = read_resource::<MySpec>(data);

    pretty_assertions::assert_eq!(
        val,
        ResourceInput {
            schema: "https://kamu.dev/schemas/core/v1/MyResource"
                .parse()
                .unwrap(),
            headers: headers("my-thing", Some("sergiimk")),
            spec: MySpec {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                },
                variables: Variables {
                    entries: BTreeMap::from_iter([(
                        "poolSize".to_string(),
                        Variable { value: "10".into() }
                    )]),
                }
            }
        }
    );

    pretty_assertions::assert_eq!(
        write_resource::<MySpec, _>(val),
        indoc!(
            r#"
            $schema: https://kamu.dev/schemas/core/v1/MyResource
            headers:
              name: my-thing
              account:
                name: sergiimk
            spec:
              secrets:
                tls:
                  value: <cert>
              variables:
                poolSize:
                  value: '10'
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_input_refs() {
    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBinding
        headers:
          name: alice-bob
        spec:
          policy:
            name: dataset-maintainer
          subject:
            kind: Principal
            type: User
            name: alice
          object:
            type: Dataset
            account: bob
            name: bobs-dataset
        "#
    );

    let val = read_resource::<serde::auth::PolicyBindingSpecInput>(data);

    pretty_assertions::assert_eq!(
        val,
        ResourceInput {
            schema: PolicyBinding::type_uri().cheap_clone(),
            headers: headers("alice-bob", None),
            spec: PolicyBindingSpecInput {
                policy: PolicyRef {
                    account: None,
                    id: None,
                    name: Some("dataset-maintainer".parse().unwrap()),
                },
                subject: SubjectRef::Principal(PrincipalRef {
                    r#type: Some(TypeRef::Name("User".parse().unwrap())),
                    account: None,
                    id: None,
                    did: None,
                    name: Some("alice".parse().unwrap()),
                }),
                object: ResourceRef {
                    account: Some(ResourceOwnerRef {
                        r#type: None,
                        id: None,
                        did: None,
                        name: Some("bob".parse().unwrap()),
                    }),
                    r#type: Some(TypeRef::Name("Dataset".parse().unwrap())),
                    id: None,
                    did: None,
                    name: Some("bobs-dataset".parse().unwrap()),
                },
            },
        }
    );

    pretty_assertions::assert_eq!(
        write_resource::<serde::auth::PolicyBindingSpecInput, _>(val),
        indoc!(
            r#"
            $schema: https://opendatafabric.org/schemas/auth/v1alpha1/PolicyBinding
            headers:
              name: alice-bob
            spec:
              policy:
                name: dataset-maintainer
              subject:
                kind: Principal
                type: User
                name: alice
              object:
                account:
                  name: bob
                type: Dataset
                name: bobs-dataset
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_canonical() {
    // Canonical resource representation
    let data = indoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
        headers:
          id: 7149c2f9-41f2-4cbb-be8f-7d7747525f9a
          name: my-secret
          account:
            type: https://opendatafabric.org/schemas/auth/v1alpha1/User
            id: c27331ce-ce88-4ff9-8c5a-4ce8107cc03f
            did: did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
            name: sergiimk
          labels:
            nested:
              a: x
              b: y
            string: foo
          annotations:
            nested:
              a: x
              b: y
            string: foo
          generation: 1
          statusGeneration: 3
          createdAt: 2026-01-01T00:00:00Z
          updatedAt: 2026-01-01T00:00:00Z
          deletionRequestedAt: 2026-01-01T00:00:00Z
          deletedAt: 2026-01-01T00:00:00Z
        spec:
          secrets:
            tls:
              value: <cert>
        status:
          phase: Ready
          observedGeneration: 1
          observedAt: 2026-01-01T00:00:00Z
          reconciledGeneration: 1
          reconciledAt: 2026-01-01T00:00:00Z
          conditions:
            https://opendatafabric.org/core/v1/Bool:
              value: true
            https://opendatafabric.org/core/v1/Nested:
              value:
                a: x
                b: y
            https://opendatafabric.org/core/v1/Str:
              lastTransitionTime: 2026-01-01T00:00:00Z
              observedGeneration: 1
              value: foo
        "#
    );

    let val =
        serde_yaml::from_str::<serde::resources::Resource<serde::config::SecretSetSpec>>(data)
            .unwrap()
            .into_dto()
            .unwrap();

    pretty_assertions::assert_eq!(
        val,
        Resource {
            schema: SecretSet::type_uri().cheap_clone(),
            headers: ResourceHeaders {
                id: "7149c2f9-41f2-4cbb-be8f-7d7747525f9a".parse().unwrap(),
                name: "my-secret".parse().unwrap(),
                account: ResourceOwnerHandle {
                    r#type: User::type_uri().cheap_clone(),
                    id: "c27331ce-ce88-4ff9-8c5a-4ce8107cc03f".parse().unwrap(),
                    did: AccountDid::from_did_str("did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4").unwrap(),
                    name: "sergiimk".parse().unwrap(),
                },
                labels: ResourceLabels {
                    entries: BTreeMap::from_iter([
                        ("string".parse().unwrap(), json!("foo")),
                        ("nested".parse().unwrap(), json!({"a": "x", "b": "y"}))
                    ])
                },
                annotations: ResourceAnnotations {
                    entries: BTreeMap::from_iter([
                        ("string".parse().unwrap(), json!("foo")),
                        ("nested".parse().unwrap(), json!({"a": "x", "b": "y"}))
                    ])
                },
                owner_references: None,
                generation: 1,
                status_generation: 3,
                created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                updated_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                deletion_requested_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                deleted_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
            },
            spec: SecretSetSpec {
                secrets: Secrets {
                    entries: BTreeMap::from_iter([(
                        "tls".to_string(),
                        Secret {
                            value: "<cert>".into(),
                            content_encoding: None,
                        }
                    )]),
                }
            },
            status: ResourceStatus {
                phase: ResourcePhase::Ready,
                observed_generation: Some(1),
                observed_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                reconciled_generation: Some(1),
                reconciled_at: Some("2026-01-01T00:00:00Z".parse().unwrap()),
                conditions: ResourceConditions {
                    entries: BTreeMap::from_iter([
                        (
                            "https://opendatafabric.org/core/v1/Str".parse().unwrap(),
                            json!({
                                "value": "foo",
                                "observedGeneration": 1,
                                "lastTransitionTime": "2026-01-01T00:00:00Z",
                            })
                        ),
                        (
                            "https://opendatafabric.org/core/v1/Bool".parse().unwrap(),
                            json!({"value": true})
                        ),
                        (
                            "https://opendatafabric.org/core/v1/Nested".parse().unwrap(),
                            json!({"value": {"a": "x", "b": "y"}})
                        )
                    ])
                },
            },
        }
    );

    pretty_assertions::assert_eq!(
        serde_yaml::to_string(
            &serde::resources::Resource::<serde::config::SecretSetSpec>::from(val.clone())
        )
        .unwrap(),
        data
    );

    // Map from canonical to input type
    let ival: ResourceInput<SecretSetSpecInput> = val.into();

    pretty_assertions::assert_eq!(
        write_resource::<serde::config::SecretSetSpecInput, _>(ival),
        indoc!(
            r#"
            $schema: https://opendatafabric.org/schemas/config/v1alpha1/SecretSet
            headers:
              id: 7149c2f9-41f2-4cbb-be8f-7d7747525f9a
              name: my-secret
              account:
                type: https://opendatafabric.org/schemas/auth/v1alpha1/User
                id: c27331ce-ce88-4ff9-8c5a-4ce8107cc03f
                did: did:odf:fed01816ef0a9abe93aba816ef0a9abe93aba90e6065747170300c0d3d30c2cd8d7a4
                name: sergiimk
              labels:
                nested:
                  a: x
                  b: y
                string: foo
              annotations:
                nested:
                  a: x
                  b: y
                string: foo
            spec:
              secrets:
                tls:
                  value: <cert>
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_resource_owner_ref() {
    let resource_id = ResourceID::new(uuid::Uuid::new_v4());
    let did = AccountDid::new_generated_ed25519().1;

    assert_eq!(
        serde_json::from_value::<serde::auth::ResourceOwnerRef>(json!({ "did": did.to_string() }))
            .unwrap()
            .into_dto()
            .unwrap(),
        ResourceOwnerRef {
            r#type: None,
            id: None,
            did: Some(did.clone()),
            name: None,
        }
    );

    assert_eq!(
        serde_json::from_value::<serde::auth::ResourceOwnerRef>(json!({ "name": "alice" }))
            .unwrap()
            .into_dto()
            .unwrap(),
        ResourceOwnerRef {
            r#type: None,
            id: None,
            did: None,
            name: Some("alice".parse().unwrap()),
        }
    );

    assert_eq!(
        serde_json::from_value::<serde::auth::ResourceOwnerRef>(
            json!({ "id": resource_id.to_string(), "did": did.to_string(), "name": "alice" })
        )
        .unwrap()
        .into_dto()
        .unwrap(),
        ResourceOwnerRef {
            r#type: None,
            id: Some(resource_id),
            did: Some(did.clone()),
            name: Some("alice".parse().unwrap()),
        }
    );

    // All fields are optional: an empty object is valid and resolves to an
    // unspecified reference (resolution failure, if any, happens downstream).
    assert_eq!(
        serde_json::from_value::<serde::auth::ResourceOwnerRef>(json!({}))
            .unwrap()
            .into_dto()
            .unwrap(),
        ResourceOwnerRef {
            r#type: None,
            id: None,
            did: None,
            name: None,
        }
    );

    let both = ResourceOwnerRef {
        r#type: None,
        id: Some(resource_id),
        did: Some(did),
        name: Some("alice".parse().unwrap()),
    };
    let round_tripped: ResourceOwnerRef =
        serde_json::to_value(serde::auth::ResourceOwnerRef::from(both.clone()))
            .and_then(serde_json::from_value::<serde::auth::ResourceOwnerRef>)
            .unwrap()
            .into_dto()
            .unwrap();
    assert_eq!(round_tripped, both);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `struct-or-string` routes a bare string through `FromStr`, so the short form
// is only reachable in YAML if that impl is real — a stub would panic rather
// than fail to compile.
#[test]
fn test_serde_resource_selector_short_form() {
    let selector: ResourceSelector = serde_yaml::from_str::<
        serde::StructOrString<serde::resources::ResourceSelector>,
    >("SecretSet:alice/app-%")
    .unwrap()
    .0
    .into_dto()
    .unwrap();

    assert_eq!(
        selector.r#type.as_ref().map(TypeRef::as_str),
        Some("SecretSet")
    );
    assert_eq!(selector.name.as_deref(), Some("app-%"));
    assert_matches!(
        selector.account,
        Some(ResourceOwnerRef { name: Some(ref n), .. }) if n.as_str() == "alice"
    );

    // A malformed short form must be a deserialization error, never a panic.
    assert_matches!(
        serde_yaml::from_str::<serde::StructOrString<serde::resources::ResourceSelector>>(
            "not a selector"
        ),
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_serde_principal_ref_short_form() {
    let principal: PrincipalRef = serde_yaml::from_str::<
        serde::StructOrString<serde::auth::PrincipalRef>,
    >("ServiceAccount:acme/ingest-prod")
    .unwrap()
    .0
    .into_dto()
    .unwrap();

    pretty_assertions::assert_eq!(
        principal,
        PrincipalRef {
            r#type: Some(TypeRef::Name("ServiceAccount".parse().unwrap())),
            account: Some(ResourceOwnerRef {
                r#type: None,
                id: None,
                did: None,
                name: Some("acme".parse().unwrap()),
            }),
            id: None,
            did: None,
            name: Some("ingest-prod".parse().unwrap()),
        }
    );

    // The type prefix is required
    assert_matches!(
        serde_yaml::from_str::<serde::StructOrString<serde::auth::PrincipalRef>>(
            "acme/ingest-prod"
        ),
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `PolicyRef` has no short form: a string must be a deserialization error, never a panic
#[test]
fn test_serde_policy_ref_rejects_short_form() {
    assert_matches!(
        serde_yaml::from_str::<serde::StructOrString<serde::auth::PolicyRef>>("dataset-maintainer"),
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reads a `ResourceInput` whose spec has the YAML form `S`
fn read_resource<S>(yaml: &str) -> <serde::resources::ResourceInput<S> as IntoDto>::Dto
where
    serde::resources::ResourceInput<S>: IntoDto + DeserializeOwned,
{
    serde_yaml::from_str::<serde::resources::ResourceInput<S>>(yaml)
        .unwrap()
        .into_dto()
        .unwrap()
}

/// Writes a `ResourceInput` with its spec in the YAML form `S`
fn write_resource<S, T>(val: ResourceInput<T>) -> String
where
    S: From<T> + Serialize,
{
    serde_yaml::to_string(&serde::resources::ResourceInput::<S>::from(val)).unwrap()
}

fn headers(name: &str, account: Option<&str>) -> ResourceHeadersInput {
    ResourceHeadersInput {
        id: None,
        name: name.parse().unwrap(),
        account: account.map(|account| account.parse().unwrap()),
        labels: None,
        annotations: None,
        owner_references: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
