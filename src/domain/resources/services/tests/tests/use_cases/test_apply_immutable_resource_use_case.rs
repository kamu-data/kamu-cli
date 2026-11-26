// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_resources::{
    ApplyResourceAccessCheck,
    ApplyResourceAccountSource,
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    ApplyResourceResult,
    ApplyResourceUseCase,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    ImmutableTestResource,
    ImmutableTestResourceSpec,
    TestTargetRef,
    TestTargets,
    register_immutable_test_resource,
};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_stored_spec_is_canonical_and_the_same_manifest_changes_nothing() {
    let harness = ImmutableResourceHarness::new();
    let canonical = spec(Some(TestTargets::ID), Some(TestTargets::NAME), Some("auto"));

    let created = harness
        .apply(spec(None, Some(TestTargets::NAME), None))
        .await;
    assert_eq!(created.outcome, ApplyResourceOutcome::Created);
    assert_eq!(created.state.spec, canonical);

    // As it was given, as it is stored, and with only what is needed
    for same in [
        spec(None, Some(TestTargets::NAME), None),
        canonical.clone(),
        spec(Some(TestTargets::ID), None, Some("auto")),
        spec(Some(TestTargets::ID), None, None),
    ] {
        let result = harness.apply(same.clone()).await;
        assert_eq!(result.outcome, ApplyResourceOutcome::Untouched, "{same:?}");
        assert_eq!(result.state.spec, canonical, "{same:?}");
        assert_eq!(
            harness.plan_action(same.clone()).await,
            ApplyResourceAction::Untouched,
            "{same:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_changed_spec_is_rejected_along_with_changed_headers() {
    let harness = ImmutableResourceHarness::new();
    let created = harness.apply(spec(Some(TestTargets::ID), None, None)).await;

    let mut changed = harness.params(spec(Some(TestTargets::ID), None, Some("manual")));
    ImmutableResourceHarness::add_label(&mut changed);

    let rejection = harness.assert_rejected(changed).await;
    assert_eq!(
        rejection.category,
        ApplyResourceRejectionCategory::ImmutableFieldChanged
    );
    assert_eq!(
        rejection.message,
        format!(
            "Spec of resource {} is immutable and cannot be changed",
            created.id
        )
    );

    let snapshot = harness.get_snapshot_by_id(&created.id).await.unwrap();
    assert_eq!(snapshot.headers.generation, 1);
    assert_eq!(snapshot.headers.labels.entries, [].into());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_changed_headers_of_an_immutable_spec_apply() {
    let harness = ImmutableResourceHarness::new();
    let created = harness.apply(spec(Some(TestTargets::ID), None, None)).await;

    let mut labelled = harness.params(spec(Some(TestTargets::ID), None, None));
    ImmutableResourceHarness::add_label(&mut labelled);
    let updated = harness.apply_params(labelled).await;

    assert_eq!(updated.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(updated.state.headers.generation, 2);
    assert_eq!(updated.state.headers.labels.entries.len(), 1);
    assert_eq!(updated.state.spec, created.state.spec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reference_to_a_renamed_or_missing_object_is_the_same_by_id_only() {
    let harness = ImmutableResourceHarness::new();
    let by_id = || spec(Some(TestTargets::ID), None, None);
    let by_name = || spec(None, Some(TestTargets::NAME), None);
    let created = harness.apply(by_name()).await;
    let as_stored = || created.state.spec.clone();

    // Renamed
    harness.targets.put(TestTargets::ID, "renamed");
    for same in [by_id(), as_stored(), spec(None, Some("renamed"), None)] {
        harness.assert_unchanged(same, &created).await;
    }
    assert_eq!(
        harness.assert_spec_rejected(by_name()).await,
        ApplyResourceRejectionCategory::ReferencedObjectMissing
    );

    // Its name is taken over
    harness.targets.put("target-2", TestTargets::NAME);
    assert_eq!(
        harness.assert_spec_rejected(by_name()).await,
        ApplyResourceRejectionCategory::ImmutableFieldChanged
    );

    // Gone
    harness.targets.remove(TestTargets::ID);
    for same in [by_id(), as_stored()] {
        harness.assert_unchanged(same, &created).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct ImmutableResourceHarness {
    base: BaseResourceServiceHarness,
    apply_uc: Arc<dyn ApplyResourceUseCase<ImmutableTestResource>>,
    targets: Arc<TestTargets>,
}

impl ImmutableResourceHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new_with_additional_dependencies(
            register_immutable_test_resource,
        );
        Self {
            apply_uc: base.catalog().get_one().unwrap(),
            targets: base.catalog().get_one().unwrap(),
            base,
        }
    }

    fn params(
        &self,
        spec: ImmutableTestResourceSpec,
    ) -> ApplyResourceParams<ImmutableTestResource> {
        ApplyResourceParams {
            account_source: ApplyResourceAccountSource::Manifest,
            access_check: ApplyResourceAccessCheck::ByGuard,
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                odf::ResourceOwnerHandle::new_test("test"),
                "res-a",
            )
            .into(),
            spec,
            provenance: BaseResourceServiceHarness::make_provenance(),
        }
    }

    fn add_label(params: &mut ApplyResourceParams<ImmutableTestResource>) {
        params
            .headers
            .labels
            .as_mut()
            .unwrap()
            .entries
            .insert("env".parse().unwrap(), "prod".into());
    }

    async fn apply(
        &self,
        spec: ImmutableTestResourceSpec,
    ) -> ApplyResourceResult<ImmutableTestResource> {
        self.apply_params(self.params(spec)).await
    }

    async fn apply_params(
        &self,
        params: ApplyResourceParams<ImmutableTestResource>,
    ) -> ApplyResourceResult<ImmutableTestResource> {
        self.apply_uc.apply(params).await.unwrap().expect_applied()
    }

    async fn plan_action(&self, spec: ImmutableTestResourceSpec) -> ApplyResourceAction {
        match self.apply_uc.plan(self.params(spec)).await.unwrap() {
            ApplyResourcePlanningDecision::Planned(plan) => plan.action,
            ApplyResourcePlanningDecision::Rejected(rejection) => panic!("{rejection:?}"),
        }
    }

    async fn assert_unchanged(
        &self,
        spec: ImmutableTestResourceSpec,
        created: &ApplyResourceResult<ImmutableTestResource>,
    ) {
        let result = self.apply(spec.clone()).await;
        assert_eq!(result.outcome, ApplyResourceOutcome::Untouched, "{spec:?}");
        assert_eq!(result.state.spec, created.state.spec, "{spec:?}");
    }

    async fn assert_spec_rejected(
        &self,
        spec: ImmutableTestResourceSpec,
    ) -> ApplyResourceRejectionCategory {
        self.assert_rejected(self.params(spec)).await.category
    }

    /// Plan and apply reject alike
    async fn assert_rejected(
        &self,
        params: ApplyResourceParams<ImmutableTestResource>,
    ) -> ApplyResourceRejection {
        let planned = self
            .apply_uc
            .plan(ApplyResourceParams {
                account_source: ApplyResourceAccountSource::Manifest,
                access_check: ApplyResourceAccessCheck::ByGuard,
                id: params.id,
                headers: params.headers.clone(),
                spec: params.spec.clone(),
                provenance: BaseResourceServiceHarness::make_provenance(),
            })
            .await
            .unwrap();
        let ApplyResourcePlanningDecision::Rejected(plan_rejection) = planned else {
            panic!("expected a rejected plan, got: {planned:?}");
        };

        let applied = self.apply_uc.apply(params).await.unwrap();
        let ApplyResourceApplicationDecision::Rejected(rejection) = applied else {
            panic!("expected a rejected apply, got: {applied:?}");
        };

        assert_eq!(plan_rejection.category, rejection.category);
        assert_eq!(plan_rejection.message, rejection.message);
        rejection
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn spec(id: Option<&str>, name: Option<&str>, mode: Option<&str>) -> ImmutableTestResourceSpec {
    ImmutableTestResourceSpec {
        target: TestTargetRef {
            id: id.map(ToString::to_string),
            name: name.map(ToString::to_string),
        },
        mode: mode.map(ToString::to_string),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
