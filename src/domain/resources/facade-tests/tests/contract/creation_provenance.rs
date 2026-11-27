// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::CreationProvenance;
use kamu_resources_facade::ResourceFacade;
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{apply_variable_set, get_resource, variable_set_ref_by_name};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-198
contract_test!(
    created_resource_records_its_creator,
    super::test_created_resource_records_its_creator
);

pub async fn test_created_resource_records_its_creator(h: &impl FacadeContractHarness) {
    let alice = h.facade_for(TestAccount::Alice);

    apply(alice.as_ref(), "one").await;
    let created = provenance(alice.as_ref()).await.unwrap();

    assert_eq!(
        created.principal.r#type,
        *odf::metadata::auth::User::type_uri()
    );
    assert_eq!(created.principal.name.as_str(), TestAccount::Alice.name());
    assert_eq!(
        created.principal.did,
        Some(h.account_id(TestAccount::Alice).into())
    );
    assert_eq!(created.origin, h.creation_origin());

    // An update keeps the record of the creation, also when it comes through
    // another boundary
    apply(h.local_facade_for(TestAccount::Alice).as_ref(), "two").await;
    assert_eq!(provenance(alice.as_ref()).await, Some(created));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_NAME: &str = "provenance-vars";

async fn apply(facade: &dyn ResourceFacade, value: &str) {
    apply_variable_set(facade, RESOURCE_NAME, &[("A", value)]).await;
}

async fn provenance(facade: &dyn ResourceFacade) -> Option<CreationProvenance> {
    let resource = get_resource(facade, variable_set_ref_by_name(RESOURCE_NAME)).await;

    CreationProvenance::from_conditions(&resource.status.conditions).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
