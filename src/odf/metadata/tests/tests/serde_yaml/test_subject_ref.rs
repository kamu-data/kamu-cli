// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use opendatafabric_metadata::auth::*;
use opendatafabric_metadata::resources::*;
use opendatafabric_metadata::serde::yaml as serde;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case(
    indoc!(
        r#"
        subject:
          kind: Principal
          type: User
          name: alice
        "#
    ),
    SubjectRef::Principal(PrincipalRef {
        r#type: Some(TypeRef::Name("User".parse().unwrap())),
        account: None,
        id: None,
        did: None,
        name: Some("alice".parse().unwrap()),
    }),
)]
#[case(
    indoc!(
        r#"
        subject:
          kind: Principal
          type: ServiceAccount
          account:
            name: alice
          name: myagent
        "#
    ),
    SubjectRef::Principal(PrincipalRef {
        r#type: Some(TypeRef::Name("ServiceAccount".parse().unwrap())),
        account: Some(ResourceOwnerRef {
            id: None,
            did: None,
            name: Some("alice".parse().unwrap()),
        }),
        id: None,
        did: None,
        name: Some("myagent".parse().unwrap()),
    }),
)]
fn test_subject_ref_full_form(#[case] data: &str, #[case] expected: SubjectRef) {
    #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    struct Wrapper {
        subject: serde::StructOrString<serde::auth::SubjectRef>,
    }

    let w = serde_yaml::from_str::<Wrapper>(data).unwrap();
    let val: SubjectRef = w.subject.try_into().unwrap();
    pretty_assertions::assert_eq!(val, expected);

    let w = Wrapper {
        subject: val.into(),
    };
    pretty_assertions::assert_eq!(serde_yaml::to_string(&w).unwrap(), data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rstest::rstest]
#[case(
    "subject: User:alice",
    indoc!(
        r#"
        subject:
          kind: Principal
          type: User
          name: alice
        "#
    ),
    SubjectRef::Principal(PrincipalRef {
        r#type: Some(TypeRef::Name("User".parse().unwrap())),
        account: None,
        id: None,
        did: None,
        name: Some("alice".parse().unwrap()),
    }),
)]
#[case(
    "subject: ServiceAccount:alice/myagent",
    indoc!(
        r#"
        subject:
          kind: Principal
          type: ServiceAccount
          account:
            name: alice
          name: myagent
        "#
    ),
    SubjectRef::Principal(PrincipalRef {
        r#type: Some(TypeRef::Name("ServiceAccount".parse().unwrap())),
        account: Some(ResourceOwnerRef {
            id: None,
            did: None,
            name: Some("alice".parse().unwrap()),
        }),
        id: None,
        did: None,
        name: Some("myagent".parse().unwrap()),
    }),
)]
#[case(
    "subject: Group:employees",
    indoc!(
        r#"
        subject:
          kind: Group
          name: employees
        "#
    ),
    SubjectRef::Group(ResourceRef {
        r#type: Some(TypeRef::Name("Group".parse().unwrap())),
        account: None,
        id: None,
        did: None,
        name: Some("employees".parse().unwrap()),
    }),
)]
#[case(
    "subject: Everyone:*",
    indoc!(
        r#"
        subject:
          kind: Everyone
        "#
    ),
    SubjectRef::Everyone,
)]
#[case(
    "subject: Anonymous:*",
    indoc!(
        r#"
        subject:
          kind: Anonymous
        "#
    ),
    SubjectRef::Anonymous,
)]
#[case(
    "subject: Authenticated:*",
    indoc!(
        r#"
        subject:
          kind: Authenticated
        "#
    ),
    SubjectRef::Authenticated,
)]
fn test_subject_ref_short_form(
    #[case] short: &str,
    #[case] full: &str,
    #[case] expected: SubjectRef,
) {
    #[derive(Debug, ::serde::Serialize, ::serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    #[serde(rename_all = "camelCase")]
    struct Wrapper {
        subject: serde::StructOrString<serde::auth::SubjectRef>,
    }

    let w = serde_yaml::from_str::<Wrapper>(short).unwrap();
    let val: SubjectRef = w.subject.try_into().unwrap();
    pretty_assertions::assert_eq!(val, expected);

    let w = Wrapper {
        subject: val.into(),
    };
    pretty_assertions::assert_eq!(serde_yaml::to_string(&w).unwrap(), full);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
