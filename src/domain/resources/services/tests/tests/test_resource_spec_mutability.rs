// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::Utc;
use event_sourcing::{Aggregate, Projection};
use kamu_resources::{
    DeclarativeResource,
    EventSourcedResource,
    ManagedResource,
    ResourceEvent,
    ResourceSpecImmutableError,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    ImmutableTestResource,
    ImmutableTestResourceLifecycleError,
    ImmutableTestResourceSpec,
    ImmutableTestResourceState,
    TestTargetRef,
};
use pretty_assertions::assert_eq;

use crate::tests::utils::make_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_immutable_spec_accepts_only_itself() {
    let mut harness = SpecMutabilityHarness::created_with("auto");

    assert_matches!(harness.try_update_spec("auto"), Ok(()));
    assert_matches!(
        harness.try_update_spec("other"),
        Err(ImmutableTestResourceLifecycleError::SpecImmutable(
            ResourceSpecImmutableError { id }
        )) if id == *harness.resource.id()
    );

    assert_eq!(harness.resource.headers().generation, 1);
    assert_eq!(
        *harness.resource.spec(),
        SpecMutabilityHarness::spec("auto")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_immutable_spec_is_not_validated_on_update() {
    // Stored before the validation refused it
    let mut harness = SpecMutabilityHarness::created_with(ImmutableTestResourceSpec::INVALID_MODE);
    assert_matches!(
        harness.try_update_spec(ImmutableTestResourceSpec::INVALID_MODE),
        Ok(())
    );

    let mut harness = SpecMutabilityHarness::created_with("auto");
    assert_matches!(
        harness.try_update_spec(ImmutableTestResourceSpec::INVALID_MODE),
        Err(ImmutableTestResourceLifecycleError::SpecImmutable(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_spec_updated_event_of_an_immutable_spec_fails_projection() {
    let harness = SpecMutabilityHarness::created_with("auto");
    let state = harness.resource.as_ref().clone();

    for mode in ["auto", "other"] {
        let event = harness.resource.make_spec_updated_event(
            Utc::now(),
            SpecMutabilityHarness::spec(mode),
            2,
        );
        assert_matches!(event, ResourceEvent::SpecUpdated(_));

        ImmutableTestResourceState::apply(Some(state.clone()), event).unwrap_err();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SpecMutabilityHarness {
    resource: ImmutableTestResource,
}

impl SpecMutabilityHarness {
    /// A resource created by a stored event, so without validation
    fn created_with(mode: &str) -> Self {
        let id = make_id();
        let event = ImmutableTestResource::make_created_event(
            Utc::now(),
            id,
            BaseResourceServiceHarness::make_headers_input(
                odf::ResourceOwnerHandle::new_test("test"),
                "res-a",
            ),
            Self::spec(mode),
        );
        Self {
            resource: ImmutableTestResource::from_aggregate(Aggregate::new(id, event).unwrap()),
        }
    }

    fn spec(mode: &str) -> ImmutableTestResourceSpec {
        ImmutableTestResourceSpec {
            target: TestTargetRef::default(),
            mode: Some(mode.to_string()),
        }
    }

    fn try_update_spec(&mut self, mode: &str) -> Result<(), ImmutableTestResourceLifecycleError> {
        <ImmutableTestResource as ManagedResource>::try_update_spec(
            &mut self.resource,
            Utc::now(),
            Self::spec(mode),
            2,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
