// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::auth::*;
use opendatafabric_metadata::resources::*;
use opendatafabric_metadata::serde::yaml as serde;
use opendatafabric_metadata::serde::yaml::IntoDto;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Proxy conversions are hand-written (`rust.dtoType`) rather than generated, so
// every field is asserted individually: a dropped one is silently discarded
// instead of failing to compile.
#[test]
fn test_serde_resource_selector() {
    let selector = ResourceSelector {
        account: Some(ResourceOwnerRef {
            id: None,
            did: None,
            name: Some("alice".parse().unwrap()),
        }),
        id: Some(ResourceID::new(uuid::Uuid::new_v4())),
        did: None,
        r#type: Some(TypeName::new_unchecked("SecretSet").into()),
        name: Some("app-%".to_string()),
        labels: Some(LabelFilter {
            entries: [("environment".to_string(), json!("production"))]
                .into_iter()
                .collect(),
        }),
    };

    let round_tripped: ResourceSelector =
        serde_json::to_value(serde::resources::ResourceSelector::from(selector.clone()))
            .and_then(serde_json::from_value::<serde::resources::ResourceSelector>)
            .unwrap()
            .into_dto()
            .unwrap();

    assert_eq!(round_tripped, selector);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// `type` became optional when ODF adopted the type-less selector. A selector
// that names no type must round-trip as `None` rather than failing the
// now-absent required-field check.
#[test]
fn test_serde_resource_selector_without_type() {
    let selector = ResourceSelector {
        name: Some("app-%".to_string()),
        ..Default::default()
    };

    let round_tripped: ResourceSelector =
        serde_json::to_value(serde::resources::ResourceSelector::from(selector.clone()))
            .and_then(serde_json::from_value::<serde::resources::ResourceSelector>)
            .unwrap()
            .into_dto()
            .unwrap();

    assert_eq!(round_tripped, selector);
    assert!(round_tripped.r#type.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
