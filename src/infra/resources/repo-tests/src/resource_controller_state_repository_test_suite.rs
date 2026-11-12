// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, LazyLock};

use chrono::{DateTime, TimeDelta, Utc};
use dill::Catalog;
use event_sourcing::EventID;
use kamu_resources::{
    ActionableResourcesQuery,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceRepository,
    ResourceSnapshot,
    TypeUri,
    new_pending_resource_status,
};

use crate::resource_repository_test_suite::make_test_snapshot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static CONTROLLED_KIND: LazyLock<TypeUri> =
    LazyLock::new(|| TypeUri::new_unchecked("ControlledKind"));
static OTHER_KIND: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("OtherKind"));

const CONTROLLER: ResourceControllerName = ResourceControllerName::new("test-controller");
const OTHER_CONTROLLER: ResourceControllerName = ResourceControllerName::new("other-controller");

const REVISION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_upsert_and_get_state(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let resource = ctx.create_resource("a").await;

    assert_eq!(ctx.get_state(&CONTROLLER, &resource).await, None);

    let first = ctx.retry_state(&CONTROLLER, &resource, ctx.now + TimeDelta::minutes(5));
    ctx.upsert(&first).await;
    assert_eq!(ctx.get_state(&CONTROLLER, &resource).await, Some(first));

    let second = ctx.done_state(&CONTROLLER, &resource, REVISION);
    ctx.upsert(&second).await;
    assert_eq!(ctx.get_state(&CONTROLLER, &resource).await, Some(second));

    assert_eq!(ctx.get_state(&OTHER_CONTROLLER, &resource).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_not_reconciled_predicate(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;

    assert_eq!(ctx.actionable(false).await, [resource.id]);

    ctx.reconcile(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, []);

    ctx.update_spec(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, [resource.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_deletion_pending_predicate(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;
    ctx.reconcile(&mut resource).await;

    ctx.request_deletion(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, [resource.id]);

    ctx.complete_deletion(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, []);
    assert_eq!(ctx.actionable(true).await, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_status_changed_predicate_only_when_reacting_to_status(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;
    ctx.reconcile(&mut resource).await;

    assert_eq!(ctx.actionable(false).await, []);
    assert_eq!(ctx.actionable(true).await, [resource.id]);

    ctx.upsert(&ctx.done_state(&CONTROLLER, &resource, REVISION))
        .await;
    assert_eq!(ctx.actionable(true).await, []);

    ctx.update_status(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, []);
    assert_eq!(ctx.actionable(true).await, [resource.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_done_suppression_lifted_by_generation_change(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;

    ctx.upsert(&ctx.done_state(&CONTROLLER, &resource, REVISION))
        .await;
    assert_eq!(ctx.actionable(false).await, []);

    ctx.update_spec(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, [resource.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_done_suppression_lifted_by_status_generation_change(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;

    ctx.upsert(&ctx.done_state(&CONTROLLER, &resource, REVISION))
        .await;
    assert_eq!(ctx.actionable(false).await, []);

    ctx.update_status(&mut resource).await;
    assert_eq!(ctx.actionable(false).await, [resource.id]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_done_suppression_lifted_by_controller_revision_bump(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut deleting = ctx.create_resource("deleting").await;
    ctx.request_deletion(&mut deleting).await;
    let mut reconciled = ctx.create_resource("reconciled").await;
    ctx.reconcile(&mut reconciled).await;

    for resource in [&deleting, &reconciled] {
        ctx.upsert(&ctx.done_state(&CONTROLLER, resource, REVISION))
            .await;
    }
    assert_eq!(ctx.actionable_at_revision(REVISION).await, []);

    // Only the resource a predicate selects comes back
    assert_eq!(
        ctx.actionable_at_revision(REVISION + 1).await,
        [deleting.id]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_not_before_suppresses_until_due(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let resource = ctx.create_resource("a").await;
    let not_before = ctx.now + TimeDelta::minutes(5);

    ctx.upsert(&ctx.retry_state(&CONTROLLER, &resource, not_before))
        .await;

    assert_eq!(ctx.actionable_at(ctx.now, REVISION).await, []);
    assert_eq!(ctx.actionable_at(ctx.now, REVISION + 1).await, []);
    assert_eq!(
        ctx.actionable_at(not_before + TimeDelta::seconds(1), REVISION)
            .await,
        [resource.id]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_retry_under_status_predicate(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;
    ctx.reconcile(&mut resource).await;
    let not_before = ctx.now + TimeDelta::minutes(5);
    let after_not_before = not_before + TimeDelta::seconds(1);

    ctx.upsert(&ctx.retry_state(&CONTROLLER, &resource, not_before))
        .await;

    assert_eq!(ctx.actionable_reacting_at(ctx.now).await, []);
    assert_eq!(
        ctx.actionable_reacting_at(after_not_before).await,
        [resource.id]
    );
    // Without the status predicate nothing selects a reconciled resource
    assert_eq!(ctx.actionable_at(after_not_before, REVISION).await, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_other_controllers_and_schemas_ignored(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let resource = ctx.create_resource("a").await;
    ctx.create_resource_of(&OTHER_KIND, "other").await;

    ctx.upsert(&ctx.done_state(&OTHER_CONTROLLER, &resource, REVISION))
        .await;
    ctx.upsert(&ctx.retry_state(
        &OTHER_CONTROLLER,
        &resource,
        ctx.now + TimeDelta::minutes(5),
    ))
    .await;

    assert_eq!(ctx.actionable(false).await, [resource.id]);
    assert_eq!(ctx.next_deadline(false).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_limit_and_distinct_ids(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let not_reconciled = ctx.create_resource("not-reconciled").await;
    // Matches both the deletion and the status predicates
    let mut deleting = ctx.create_resource("deleting").await;
    ctx.request_deletion(&mut deleting).await;
    let mut status_changed = ctx.create_resource("status-changed").await;
    ctx.reconcile(&mut status_changed).await;

    let mut all = vec![not_reconciled.id, deleting.id, status_changed.id];
    all.sort();
    assert_eq!(ctx.actionable(true).await, all);

    for limit in 1..=2 {
        let limited = ctx.actionable_limited(true, limit).await;
        assert_eq!(limited.len(), limit);
        assert!(limited.iter().all(|id| all.contains(id)));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_next_deadline(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let early = ctx.create_resource("early").await;
    let late = ctx.create_resource("late").await;
    let mut reconciled = ctx.create_resource("reconciled").await;
    ctx.reconcile(&mut reconciled).await;

    assert_eq!(ctx.next_deadline(false).await, None);

    let past = ctx.now - TimeDelta::minutes(1);
    let future = ctx.now + TimeDelta::minutes(5);
    ctx.upsert(&ctx.retry_state(&CONTROLLER, &late, future))
        .await;
    assert_eq!(ctx.next_deadline(false).await, Some(future));

    ctx.upsert(&ctx.retry_state(&CONTROLLER, &early, past))
        .await;
    assert_eq!(ctx.next_deadline(false).await, Some(past));

    // No predicate selects a reconciled resource, unless the controller reacts
    // to status and has not handled it
    let earliest = ctx.now - TimeDelta::minutes(10);
    ctx.upsert(&ctx.retry_state(&CONTROLLER, &reconciled, earliest))
        .await;
    assert_eq!(ctx.next_deadline(false).await, Some(past));
    assert_eq!(ctx.next_deadline(true).await, Some(earliest));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_next_deadline_ignores_deleted_resources(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let mut resource = ctx.create_resource("a").await;
    ctx.request_deletion(&mut resource).await;
    let not_before = ctx.now + TimeDelta::minutes(5);

    ctx.upsert(&ctx.retry_state(&CONTROLLER, &resource, not_before))
        .await;
    assert_eq!(ctx.next_deadline(true).await, Some(not_before));

    ctx.complete_deletion(&mut resource).await;
    assert_eq!(ctx.next_deadline(true).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_count_failing_resources(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let failing = ctx.create_resource("failing").await;
    let recovered = ctx.create_resource("recovered").await;
    let mut deleted = ctx.create_resource("deleted").await;
    let retrying = ctx.create_resource("retrying").await;

    assert_eq!(ctx.count_failing(&CONTROLLER).await, 0);

    let retry_at = ctx.now + TimeDelta::minutes(5);
    for resource in [&failing, &recovered, &deleted, &retrying] {
        ctx.upsert(&ctx.retry_state(&CONTROLLER, resource, retry_at))
            .await;
    }
    ctx.upsert(&ctx.retry_state(&OTHER_CONTROLLER, &failing, retry_at))
        .await;
    assert_eq!(ctx.count_failing(&CONTROLLER).await, 4);

    // A success resets the failures, a planned retry is not one
    ctx.upsert(&ctx.done_state(&CONTROLLER, &recovered, REVISION))
        .await;
    let mut planned_retry = ctx.retry_state(&CONTROLLER, &retrying, retry_at);
    planned_retry.attempts = 0;
    planned_retry.last_error = None;
    ctx.upsert(&planned_retry).await;
    assert_eq!(ctx.count_failing(&CONTROLLER).await, 2);

    ctx.complete_deletion(&mut deleted).await;
    assert_eq!(ctx.count_failing(&CONTROLLER).await, 1);
    assert_eq!(ctx.count_failing(&OTHER_CONTROLLER).await, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_states_for_resource(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let removed = ctx.create_resource("removed").await;
    let kept = ctx.create_resource("kept").await;

    for controller in [&CONTROLLER, &OTHER_CONTROLLER] {
        for resource in [&removed, &kept] {
            ctx.upsert(&ctx.done_state(controller, resource, REVISION))
                .await;
        }
    }

    ctx.delete_states(&removed).await;

    for controller in [&CONTROLLER, &OTHER_CONTROLLER] {
        assert_eq!(ctx.get_state(controller, &removed).await, None);
        assert_eq!(
            ctx.get_state(controller, &kept).await,
            Some(ctx.done_state(controller, &kept, REVISION))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_upsert_state_with_nul_in_error(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let resource = ctx.create_resource("a").await;

    let mut state = ctx.retry_state(&CONTROLLER, &resource, ctx.now + TimeDelta::minutes(5));
    state.last_error = Some("before\0after".to_string());
    ctx.upsert(&state).await;

    let stored = ctx.get_state(&CONTROLLER, &resource).await.unwrap();
    let last_error = stored.last_error.unwrap();
    assert!(last_error.starts_with("before"), "{last_error}");
    assert!(last_error.ends_with("after"), "{last_error}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_upsert_state_of_unknown_resource_fails(catalog: &Catalog) {
    let ctx = ControllerStateTestContext::new(catalog);
    let unknown = ctx.unsaved_resource("unknown").await;

    let state = ctx.done_state(&CONTROLLER, &unknown, REVISION);
    assert!(ctx.states.upsert_state(&state).await.is_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ControllerStateTestContext {
    resources: Arc<dyn ResourceRepository>,
    states: Arc<dyn ResourceControllerStateRepository>,
    account: odf::ResourceOwnerHandle,
    /// Whole seconds, so a timestamp survives every backend unchanged
    now: DateTime<Utc>,
}

impl ControllerStateTestContext {
    fn new(catalog: &Catalog) -> Self {
        Self {
            resources: catalog.get_one::<dyn ResourceRepository>().unwrap(),
            states: catalog
                .get_one::<dyn ResourceControllerStateRepository>()
                .unwrap(),
            account: odf::ResourceOwnerHandle::new_test("controller-owner"),
            now: DateTime::from_timestamp(Utc::now().timestamp(), 0).unwrap(),
        }
    }

    async fn create_resource(&self, name: &str) -> ResourceSnapshot {
        self.create_resource_of(&CONTROLLED_KIND, name).await
    }

    /// A resource with an ID of its own that the storage never saw
    async fn unsaved_resource(&self, name: &str) -> ResourceSnapshot {
        self.unsaved_resource_of(&CONTROLLED_KIND, name).await
    }

    async fn unsaved_resource_of(&self, schema: &TypeUri, name: &str) -> ResourceSnapshot {
        let mut snapshot = make_test_snapshot(&self.account, schema, name);
        snapshot.id = self.resources.new_resource_id().await.unwrap();
        snapshot
    }

    async fn create_resource_of(&self, schema: &TypeUri, name: &str) -> ResourceSnapshot {
        let mut snapshot = self.unsaved_resource_of(schema, name).await;
        // As apply creates resources: the first generation is 1
        snapshot.headers.generation = 1;
        snapshot.status = Some(new_pending_resource_status());
        snapshot.last_event_id = Some(EventID::new(1));
        self.resources.create_resource(&snapshot).await.unwrap();
        snapshot
    }

    /// Records a successful reconciliation of the current generation
    async fn reconcile(&self, snapshot: &mut ResourceSnapshot) {
        let generation = snapshot.headers.generation;
        let status = snapshot.status.as_mut().unwrap();
        status.observed_generation = Some(generation);
        status.reconciled_generation = Some(generation);
        snapshot.headers.status_generation += 1;
        self.save(snapshot).await;
    }

    async fn update_spec(&self, snapshot: &mut ResourceSnapshot) {
        snapshot.headers.generation += 1;
        self.save(snapshot).await;
    }

    /// A status write by another writer, such as a condition patch
    async fn update_status(&self, snapshot: &mut ResourceSnapshot) {
        snapshot.headers.status_generation += 1;
        self.save(snapshot).await;
    }

    async fn request_deletion(&self, snapshot: &mut ResourceSnapshot) {
        snapshot.headers.deletion_requested_at = Some(self.now);
        snapshot.headers.status_generation += 1;
        self.save(snapshot).await;
    }

    async fn complete_deletion(&self, snapshot: &mut ResourceSnapshot) {
        snapshot.headers.deleted_at = Some(self.now);
        snapshot.headers.status_generation += 1;
        self.save(snapshot).await;
    }

    async fn save(&self, snapshot: &mut ResourceSnapshot) {
        let expected_last_event_id = snapshot.last_event_id;
        snapshot.last_event_id = Some(EventID::new(
            expected_last_event_id.unwrap().into_inner() + 1,
        ));
        self.resources
            .update_resource(snapshot, expected_last_event_id)
            .await
            .unwrap();
    }

    /// The row the runtime writes after `Done` at the resource's current
    /// versions
    fn done_state(
        &self,
        controller: &ResourceControllerName,
        snapshot: &ResourceSnapshot,
        controller_revision: u32,
    ) -> ResourceControllerState {
        ResourceControllerState {
            controller: controller.clone(),
            resource_id: snapshot.id,
            controller_revision,
            handled_generation: Some(snapshot.headers.generation),
            handled_status_generation: Some(snapshot.headers.status_generation),
            not_before: None,
            attempts: 0,
            last_error: None,
        }
    }

    /// The row the runtime writes after a failed attempt
    fn retry_state(
        &self,
        controller: &ResourceControllerName,
        snapshot: &ResourceSnapshot,
        not_before: DateTime<Utc>,
    ) -> ResourceControllerState {
        ResourceControllerState {
            controller: controller.clone(),
            resource_id: snapshot.id,
            controller_revision: REVISION,
            handled_generation: None,
            handled_status_generation: None,
            not_before: Some(not_before),
            attempts: 2,
            last_error: Some("transient failure".to_string()),
        }
    }

    async fn upsert(&self, state: &ResourceControllerState) {
        self.states.upsert_state(state).await.unwrap();
    }

    async fn delete_states(&self, snapshot: &ResourceSnapshot) {
        self.states
            .delete_states_for_resource(&snapshot.id)
            .await
            .unwrap();
    }

    async fn get_state(
        &self,
        controller: &ResourceControllerName,
        snapshot: &ResourceSnapshot,
    ) -> Option<ResourceControllerState> {
        self.states
            .get_state(controller, &snapshot.id)
            .await
            .unwrap()
    }

    fn query(&self, controller_revision: u32, reacts_to_status: bool) -> ActionableResourcesQuery {
        ActionableResourcesQuery {
            controller: CONTROLLER,
            schema: CONTROLLED_KIND.clone(),
            controller_revision,
            reacts_to_status,
        }
    }

    async fn actionable(&self, reacts_to_status: bool) -> Vec<ResourceID> {
        self.find_actionable(self.query(REVISION, reacts_to_status), self.now, 100)
            .await
    }

    async fn actionable_limited(&self, reacts_to_status: bool, limit: usize) -> Vec<ResourceID> {
        self.find_actionable(self.query(REVISION, reacts_to_status), self.now, limit)
            .await
    }

    async fn actionable_at_revision(&self, controller_revision: u32) -> Vec<ResourceID> {
        self.actionable_at(self.now, controller_revision).await
    }

    async fn actionable_at(&self, now: DateTime<Utc>, controller_revision: u32) -> Vec<ResourceID> {
        self.find_actionable(self.query(controller_revision, false), now, 100)
            .await
    }

    async fn actionable_reacting_at(&self, now: DateTime<Utc>) -> Vec<ResourceID> {
        self.find_actionable(self.query(REVISION, true), now, 100)
            .await
    }

    /// Sorted, as the repository promises no order
    async fn find_actionable(
        &self,
        query: ActionableResourcesQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Vec<ResourceID> {
        let mut resource_ids = self
            .states
            .find_actionable_resource_ids(&query, now, limit)
            .await
            .unwrap();
        resource_ids.sort();
        resource_ids
    }

    async fn count_failing(&self, controller: &ResourceControllerName) -> usize {
        self.states
            .count_failing_resources(controller)
            .await
            .unwrap()
    }

    async fn next_deadline(&self, reacts_to_status: bool) -> Option<DateTime<Utc>> {
        self.states
            .find_next_deadline(&self.query(REVISION, reacts_to_status))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
