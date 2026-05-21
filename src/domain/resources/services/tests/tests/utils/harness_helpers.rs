// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_resources::{
    ApplyResourceParams,
    ReconcilableResource,
    ReconcilableResourceEvent,
    ResourceEventCreated,
    ResourceEventSpecUpdated,
    ResourceUID,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{TestResource, TestResourceSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_account_id() -> odf::AccountID {
    odf::AccountID::new_generated_ed25519().1
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_resource_params(
    account_id: odf::AccountID,
    name: &str,
) -> ApplyResourceParams<TestResource> {
    ApplyResourceParams {
        uid: None,
        metadata: BaseResourceServiceHarness::make_metadata_input(account_id, name),
        spec: TestResourceSpec {
            value: name.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_uid() -> ResourceUID {
    ResourceUID::new(uuid::Uuid::new_v4())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_fresh_aggregate(
    account_id: odf::AccountID,
    name: &str,
) -> (ResourceUID, crate::tests::utils::TestResource) {
    use crate::tests::utils::TestResourceSpec;
    let uid = make_uid();
    let metadata = BaseResourceServiceHarness::make_metadata_input(account_id, name);
    let spec = TestResourceSpec {
        value: name.to_string(),
    };
    let agg =
        crate::tests::utils::TestResource::try_create(Utc::now(), uid, metadata, spec).unwrap();
    (uid, agg)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TestEvent = ReconcilableResourceEvent<TestResourceSpec, (), String>;

pub fn make_created_event(uid: ResourceUID, name: &str, value: &str) -> TestEvent {
    let account_id = make_account_id();
    TestEvent::Created(ResourceEventCreated {
        event_time: Utc::now(),
        uid,
        metadata: BaseResourceServiceHarness::make_metadata_input(account_id, name),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    })
}

pub fn make_spec_updated_event(uid: ResourceUID, value: &str, new_generation: u64) -> TestEvent {
    TestEvent::SpecUpdated(ResourceEventSpecUpdated {
        event_time: Utc::now(),
        uid,
        new_spec: TestResourceSpec {
            value: value.to_string(),
        },
        new_generation,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
