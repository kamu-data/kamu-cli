// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{SECRET_SET_KIND, VARIABLE_SET_KIND};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-001
contract_test!(lists_supported_kinds, super::test_lists_supported_kinds);

pub async fn test_lists_supported_kinds(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let descriptors = facade.list_supported_kinds().await.unwrap();

    assert!(!descriptors.is_empty(), "supported kinds must not be empty");

    for d in &descriptors {
        assert!(!d.name.is_empty(), "descriptor name must not be empty");
        assert!(!d.kind.is_empty(), "descriptor kind must not be empty");
        assert!(
            !d.api_version.is_empty(),
            "descriptor api_version must not be empty"
        );
    }

    // Descriptor names are unique
    let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
    let name_count = names.len();
    let unique_names: std::collections::HashSet<_> = names.into_iter().collect();
    assert_eq!(
        unique_names.len(),
        name_count,
        "descriptor names must be unique"
    );

    // (kind, api_version) pairs are unique
    let pairs: Vec<_> = descriptors
        .iter()
        .map(|d| (d.kind.as_str(), d.api_version.as_str()))
        .collect();
    let pair_count = pairs.len();
    let unique_pairs: std::collections::HashSet<_> = pairs.into_iter().collect();
    assert_eq!(
        unique_pairs.len(),
        pair_count,
        "(kind, api_version) pairs must be unique"
    );

    // VariableSet and SecretSet must be present
    let has_variable_set = descriptors.iter().any(|d| d.kind == VARIABLE_SET_KIND);
    let has_secret_set = descriptors.iter().any(|d| d.kind == SECRET_SET_KIND);
    assert!(has_variable_set, "VariableSet kind must be present");
    assert!(has_secret_set, "SecretSet kind must be present");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
