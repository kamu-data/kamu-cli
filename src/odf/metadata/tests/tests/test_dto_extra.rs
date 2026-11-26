// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_metadata_event_type_flags_consts() {
    use opendatafabric_metadata::datasets::MetadataEventTypeFlags;

    assert_eq!(3, MetadataEventTypeFlags::DATA_BLOCK.bits());
    assert_eq!(8188, MetadataEventTypeFlags::KEY_BLOCK.bits());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_resource_owner_ref_matches() {
    use opendatafabric_metadata::auth::{
        AccountDid,
        AccountName,
        ResourceOwnerHandle,
        ResourceOwnerRef,
    };
    use opendatafabric_metadata::resources::{ResourceID, TypeName, TypeUri};

    let user_type = TypeUri::new_unchecked("https://opendatafabric.org/schemas/auth/v1alpha1/User");
    let handle = ResourceOwnerHandle {
        r#type: user_type.clone(),
        id: ResourceID::new(uuid::Uuid::from_u128(1)),
        did: AccountDid::new_seeded_ed25519(b"alice"),
        name: AccountName::new_unchecked("alice"),
    };

    let cases = [
        ("no fields", ResourceOwnerRef::default(), true),
        (
            "every field",
            ResourceOwnerRef {
                r#type: Some(user_type.into()),
                id: Some(handle.id),
                did: Some(handle.did.clone()),
                name: Some(handle.name.clone()),
            },
            true,
        ),
        (
            "name in another case",
            ResourceOwnerRef {
                name: Some(AccountName::new_unchecked("Alice")),
                ..Default::default()
            },
            true,
        ),
        (
            "type by short name",
            ResourceOwnerRef {
                r#type: Some(TypeName::new_unchecked("User").into()),
                ..Default::default()
            },
            true,
        ),
        (
            "another type by short name",
            ResourceOwnerRef {
                r#type: Some(TypeName::new_unchecked("Organization").into()),
                ..Default::default()
            },
            false,
        ),
        (
            "another type URI",
            ResourceOwnerRef {
                r#type: Some(
                    TypeUri::new_unchecked(
                        "https://opendatafabric.org/schemas/auth/v1alpha1/Organization",
                    )
                    .into(),
                ),
                ..Default::default()
            },
            false,
        ),
        (
            "another resource ID",
            ResourceOwnerRef {
                id: Some(ResourceID::new(uuid::Uuid::from_u128(2))),
                ..Default::default()
            },
            false,
        ),
        (
            "another DID",
            ResourceOwnerRef {
                did: Some(AccountDid::new_seeded_ed25519(b"bob")),
                ..Default::default()
            },
            false,
        ),
        (
            "another name",
            ResourceOwnerRef {
                name: Some(AccountName::new_unchecked("bob")),
                ..Default::default()
            },
            false,
        ),
    ];

    for (case, owner_ref, expected) in cases {
        assert_eq!(owner_ref.matches(&handle), expected, "{case}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
