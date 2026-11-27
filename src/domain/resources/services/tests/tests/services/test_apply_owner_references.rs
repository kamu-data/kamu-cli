// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use kamu_resources::{
    ApplyResourceAction,
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourcePlanningDecision,
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    ApplyResourceUseCase,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    InvalidOwnerReferenceError,
    ResourceHandle,
    ResourceHeaders,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceOwnerProjectionRepository,
    ResourceRef,
    ResourceSchemaProvider,
    TypeUri,
    owner_ids,
};
use kamu_resources_services::testing::TestResource;
use odf::metadata::auth::ResourceOwnerRef;
use pretty_assertions::assert_eq;

use crate::tests::utils::{ServiceLayerTestHarness, make_id, make_resource_params};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_stores_owners() {
    let harness = OwnerReferencesHarness::new();
    let first = harness.create("first").await;
    let second = harness.create("second").await;

    let owned = harness
        .create_owned("owned", vec![by_id(first.id), by_name("SECOND")])
        .await;

    assert_eq!(
        owned.owner_references,
        Some(vec![make_handle(&first), make_handle(&second)])
    );
    assert_eq!(harness.stored_headers(owned.id).await, owned);
    assert_eq!(
        harness.indexed_owners(owned.id).await,
        sorted([first.id, second.id])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_full_reference_of_the_owner_is_accepted() {
    let harness = OwnerReferencesHarness::new();
    let owner = harness.create("owner").await;

    let owned = harness
        .create_owned("owned", vec![make_handle(&owner).into()])
        .await;

    assert_eq!(owned.owner_references, Some(vec![make_handle(&owner)]));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reference_that_is_not_an_owner_is_rejected() {
    let harness = OwnerReferencesHarness::new();
    let owner = harness.create("owner").await;
    let foreign = harness.create_in("other-account", "foreign").await;
    let deleting = harness.create("deleting").await;
    harness.request_deletion_of(deleting.id).await;
    let deleted = harness.create("deleted").await;
    harness.delete(deleted.id).await;
    let other_account: ResourceOwnerRef =
        odf::ResourceOwnerHandle::new_test("other-account").into();

    use ApplyResourceRejectionCategory as C;
    for (owner_references, category) in [
        (vec![by_id(make_id())], C::ReferencedObjectMissing),
        (vec![by_name("missing")], C::ReferencedObjectMissing),
        (vec![by_id(foreign.id)], C::ReferencedObjectMissing),
        (vec![by_name("foreign")], C::ReferencedObjectMissing),
        (vec![by_id(deleting.id)], C::ReferencedObjectMissing),
        (vec![by_name("deleting")], C::ReferencedObjectMissing),
        (vec![by_id(deleted.id)], C::ReferencedObjectMissing),
        (
            vec![ResourceRef {
                account: Some(other_account.clone()),
                ..by_name("owner")
            }],
            C::BusinessValidationFailed,
        ),
        (
            vec![ResourceRef {
                account: Some(other_account.clone()),
                ..by_id(owner.id)
            }],
            C::BusinessValidationFailed,
        ),
        (
            vec![ResourceRef {
                id: Some(owner.id),
                ..by_name("another-name")
            }],
            C::BusinessValidationFailed,
        ),
        (
            vec![ResourceRef {
                r#type: Some(TypeUri::new_unchecked("https://example.com/schemas/a/v1/B").into()),
                ..by_id(owner.id)
            }],
            C::BusinessValidationFailed,
        ),
        (
            vec![ResourceRef {
                r#type: Some("AnotherType".parse().unwrap()),
                ..by_id(owner.id)
            }],
            C::BusinessValidationFailed,
        ),
        (
            vec![by_id(owner.id), by_name("owner")],
            C::BusinessValidationFailed,
        ),
    ] {
        let rejection = harness
            .apply_rejected("owned", owner_references.clone())
            .await;
        assert_eq!(rejection.category, category, "{}", rejection.message);

        let planned = harness.plan_rejected("owned", owner_references).await;
        assert_eq!(planned.message, rejection.message);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resource_cannot_own_itself() {
    let harness = OwnerReferencesHarness::new();
    let resource = harness.create("res").await;

    for owner_reference in [by_id(resource.id), by_name("res")] {
        let rejection = harness.apply_rejected("res", vec![owner_reference]).await;
        assert_eq!(
            rejection.category,
            ApplyResourceRejectionCategory::BusinessValidationFailed
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unresolvable_form_is_invalid() {
    let harness = OwnerReferencesHarness::new();

    for owner_reference in [
        ResourceRef::default(),
        ResourceRef {
            r#type: None,
            ..by_name("owner")
        },
        ResourceRef {
            r#type: Some("UnresolvedName".parse().unwrap()),
            ..by_name("owner")
        },
    ] {
        let result = harness
            .apply_svc
            .apply(harness.make_params("owned", vec![owner_reference]))
            .await;
        assert_matches!(
            result,
            Err(ApplyResourceUseCaseError::InvalidHeaders(
                ResourceHeadersValidationError::InvalidOwnerReference(
                    InvalidOwnerReferenceError::Empty
                        | InvalidOwnerReferenceError::UntypedName
                        | InvalidOwnerReferenceError::UnknownType(_)
                )
            ))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_changed_owners_bump_generation() {
    let harness = OwnerReferencesHarness::new();
    let first = harness.create("first").await.id;
    let second = harness.create("second").await.id;
    let third = harness.create("third").await.id;
    let owned = harness
        .create_owned("owned", vec![by_id(first), by_id(second)])
        .await;
    assert_eq!(owned.generation, 1);

    // Another order, and another way to name the same owner
    for same in [
        vec![by_id(second), by_id(first)],
        vec![by_id(first), by_name("second")],
    ] {
        let (outcome, headers) = harness.apply("owned", same).await;
        assert_eq!(outcome, ApplyResourceOutcome::Untouched);
        assert_eq!(headers, owned);
    }

    let (outcome, headers) = harness
        .apply("owned", vec![by_id(third), by_id(first)])
        .await;
    assert_eq!(outcome, ApplyResourceOutcome::Updated);
    assert_eq!(headers.generation, 2);
    assert_eq!(harness.stored_headers(owned.id).await, headers);
    assert_eq!(
        harness.indexed_owners(owned.id).await,
        sorted([first, third])
    );

    // Omitted as well as empty
    let (_, headers) = harness.apply("owned", vec![]).await;
    assert_eq!(headers.generation, 3);
    assert_eq!(headers.owner_references, None);
    assert_eq!(harness.indexed_owners(owned.id).await, []);

    harness.apply("owned", vec![by_id(first)]).await;
    let result = harness
        .apply_svc
        .apply(make_resource_params(
            harness.account_handle.clone(),
            "owned",
        ))
        .await
        .unwrap()
        .expect_applied();
    assert_eq!(result.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(result.state.headers.owner_references, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_owner_is_kept_whatever_became_of_it() {
    let harness = OwnerReferencesHarness::new();
    let renamed = harness.create("renamed").await;
    let deleted = harness.create("deleted").await;
    let owned = harness
        .create_owned("owned", vec![by_id(renamed.id), by_id(deleted.id)])
        .await;

    harness.rename(renamed.id, "new-name").await;
    harness.delete(deleted.id).await;

    // As a manifest rendered from the resource names them, and by ID alone
    let rendered = owned.owner_references.iter().flatten().cloned();
    for same in [
        rendered.map(ResourceRef::from).collect(),
        vec![by_id(renamed.id), by_id(deleted.id)],
    ] {
        let (outcome, headers) = harness.apply("owned", same).await;
        assert_eq!(outcome, ApplyResourceOutcome::Untouched);
        assert_eq!(headers, owned);
    }

    // Also next to a new one
    let added = harness.create("added").await;
    let (_, headers) = harness
        .apply("owned", vec![by_id(deleted.id), by_id(added.id)])
        .await;
    assert_eq!(
        headers.owner_references,
        Some(vec![make_handle(&deleted), make_handle(&added)])
    );

    // Dropped, it can't be added again
    harness.apply("owned", vec![by_id(added.id)]).await;
    let rejection = harness
        .apply_rejected("owned", vec![by_id(deleted.id)])
        .await;
    assert_eq!(
        rejection.category,
        ApplyResourceRejectionCategory::ReferencedObjectMissing
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_owner_of_another_account_or_type_is_rejected() {
    let harness = OwnerReferencesHarness::new();
    let owner = harness.create("owner").await;
    harness.create_owned("owned", vec![by_id(owner.id)]).await;
    harness.delete(owner.id).await;

    for mismatching in [
        ResourceRef {
            account: Some(odf::ResourceOwnerHandle::new_test("other-account").into()),
            ..by_id(owner.id)
        },
        ResourceRef {
            r#type: Some("AnotherType".parse().unwrap()),
            ..by_id(owner.id)
        },
    ] {
        let rejection = harness.apply_rejected("owned", vec![mismatching]).await;
        assert_eq!(
            rejection.category,
            ApplyResourceRejectionCategory::BusinessValidationFailed
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_events_record_no_empty_list() {
    let harness = OwnerReferencesHarness::new();

    let resource = harness.create_owned("res", vec![]).await;

    assert_eq!(harness.created_event_owners(resource.id).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_plan_shows_resolved_owners() {
    let harness = OwnerReferencesHarness::new();
    let owner = harness.create("owner").await;

    let decision = harness
        .apply_svc
        .plan(harness.make_params("owned", vec![by_name("owner")]))
        .await
        .unwrap();

    let ApplyResourcePlanningDecision::Planned(plan) = decision else {
        panic!("plan rejected");
    };
    assert_eq!(plan.action, ApplyResourceAction::Create);
    assert_eq!(
        plan.state.headers.owner_references,
        Some(vec![make_handle(&owner)])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ServiceLayerTestHarness, base)]
struct OwnerReferencesHarness {
    base: ServiceLayerTestHarness,
    apply_svc: Arc<dyn ApplyResourceUseCase<TestResource>>,
    account_handle: odf::ResourceOwnerHandle,
}

impl OwnerReferencesHarness {
    fn new() -> Self {
        let base = ServiceLayerTestHarness::new();

        Self {
            apply_svc: base.apply_test_uc(),
            base,
            account_handle: odf::ResourceOwnerHandle::new_test("test"),
        }
    }

    fn make_params(
        &self,
        name: &str,
        owner_references: Vec<ResourceRef>,
    ) -> ApplyResourceParams<TestResource> {
        let mut params = make_resource_params(self.account_handle.clone(), name);
        params.headers.owner_references = Some(owner_references);
        params
    }

    async fn create(&self, name: &str) -> ResourceHeaders {
        self.create_owned(name, vec![]).await
    }

    async fn create_in(&self, account_name: &str, name: &str) -> ResourceHeaders {
        let account = odf::ResourceOwnerHandle::new_test(account_name);
        let decision = self.apply_svc.apply(make_resource_params(account, name));
        decision
            .await
            .unwrap()
            .expect_applied()
            .state
            .headers
            .clone()
    }

    async fn create_owned(
        &self,
        name: &str,
        owner_references: Vec<ResourceRef>,
    ) -> ResourceHeaders {
        let (outcome, headers) = self.apply(name, owner_references).await;
        assert_eq!(outcome, ApplyResourceOutcome::Created);
        headers
    }

    async fn apply(
        &self,
        name: &str,
        owner_references: Vec<ResourceRef>,
    ) -> (ApplyResourceOutcome, ResourceHeaders) {
        let params = self.make_params(name, owner_references);
        let result = self.apply_svc.apply(params).await.unwrap().expect_applied();
        (result.outcome, result.state.headers.clone())
    }

    async fn apply_rejected(
        &self,
        name: &str,
        owner_references: Vec<ResourceRef>,
    ) -> ApplyResourceRejection {
        let params = self.make_params(name, owner_references);
        match self.apply_svc.apply(params).await.unwrap() {
            ApplyResourceApplicationDecision::Rejected(rejection) => rejection,
            ApplyResourceApplicationDecision::Applied(result) => {
                panic!("applied: {:?}", result.state.headers)
            }
        }
    }

    async fn plan_rejected(
        &self,
        name: &str,
        owner_references: Vec<ResourceRef>,
    ) -> ApplyResourceRejection {
        let params = self.make_params(name, owner_references);
        match self.apply_svc.plan(params).await.unwrap() {
            ApplyResourcePlanningDecision::Rejected(rejection) => rejection,
            ApplyResourcePlanningDecision::Planned(plan) => {
                panic!("planned: {:?}", plan.state.headers)
            }
        }
    }

    async fn rename(&self, id: ResourceID, name: &str) {
        let mut params = self.make_params(name, vec![]);
        params.id = Some(id);
        self.apply_svc.apply(params).await.unwrap().expect_applied();
    }

    async fn delete(&self, id: ResourceID) {
        self.request_deletion_of(id).await;
        self.complete_deletion_of(id).await;
    }

    /// Headers replayed from the events
    async fn stored_headers(&self, id: ResourceID) -> ResourceHeaders {
        let headers = self.load(id).await.headers().clone();
        let snapshot = self.get_snapshot_by_id(&id).await.unwrap();
        assert_eq!(
            owner_ids(snapshot.headers.owner_references.as_ref()),
            owner_ids(headers.owner_references.as_ref())
        );
        headers
    }

    async fn created_event_owners(&self, id: ResourceID) -> Option<Vec<ResourceRef>> {
        self.created_event(id).await.headers.owner_references
    }

    async fn indexed_owners(&self, id: ResourceID) -> Vec<ResourceID> {
        self.catalog()
            .get_one::<dyn ResourceOwnerProjectionRepository>()
            .unwrap()
            .find_owners(&id)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_id(id: ResourceID) -> ResourceRef {
    ResourceRef {
        id: Some(id),
        ..ResourceRef::default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceRef {
    ResourceRef {
        r#type: Some(TestResource::type_uri().cheap_clone().into()),
        name: Some(name.parse().unwrap()),
        ..ResourceRef::default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_handle(owner: &ResourceHeaders) -> ResourceHandle {
    ResourceHandle {
        account: owner.account.clone(),
        r#type: TestResource::type_uri().cheap_clone(),
        id: owner.id,
        did: None,
        name: owner.name.clone(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn sorted<const N: usize>(mut ids: [ResourceID; N]) -> [ResourceID; N] {
    ids.sort();
    ids
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
