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
    ResourceID,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{TestResource, TestResourceSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_resource_params(
    account: odf::AccountHandle,
    name: &str,
) -> ApplyResourceParams<TestResource> {
    ApplyResourceParams {
        id: None,
        headers: BaseResourceServiceHarness::make_headers_input(account, name),
        spec: TestResourceSpec {
            value: name.to_string(),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_id() -> ResourceID {
    ResourceID::new(uuid::Uuid::new_v4())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_fresh_aggregate(
    account: odf::AccountHandle,
    name: &str,
) -> (ResourceID, crate::tests::utils::TestResource) {
    use crate::tests::utils::TestResourceSpec;
    let id = make_id();
    let headers = BaseResourceServiceHarness::make_headers_input(account, name);
    let spec = TestResourceSpec {
        value: name.to_string(),
    };
    let agg = crate::tests::utils::TestResource::try_create(Utc::now(), id, headers, spec).unwrap();
    (id, agg)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TestEvent = ReconcilableResourceEvent<TestResourceSpec, (), String>;

pub fn make_created_event(id: ResourceID, name: &str, value: &str) -> TestEvent {
    TestEvent::Created(ResourceEventCreated {
        event_time: Utc::now(),
        id,
        headers: BaseResourceServiceHarness::make_headers_input(
            odf::AccountHandle::new_test("test"),
            name,
        ),
        spec: TestResourceSpec {
            value: value.to_string(),
        },
    })
}

pub fn make_spec_updated_event(id: ResourceID, value: &str, new_generation: u64) -> TestEvent {
    TestEvent::SpecUpdated(ResourceEventSpecUpdated {
        event_time: Utc::now(),
        id,
        new_spec: TestResourceSpec {
            value: value.to_string(),
        },
        new_generation,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
