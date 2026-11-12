// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use event_sourcing::Aggregate;
use kamu_configuration::{
    SecretSetResource,
    SecretSetSpecInput,
    VariableSetResource,
    VariableSetSpecInput,
};
use kamu_configuration_services::SECRET_SET_CONTROLLER_NAME;
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::{
    ApplyResourceParams,
    EventSourcedResource,
    RequestResourcesDeletionUseCase,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourcePersistenceService,
    ResourcePhase,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// The real types on the controller runtime. Their stuck states are written
// without the runtime, as a crash or a lost message would leave them; the
// first drain afterwards, as after a restart, finds them in storage.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_variable_set_left_reconciling_is_reconciled_after_restart() {
    let harness = ControllersHarness::new().await;

    let id = harness
        .apply_variable_set_unsettled("vars", [("A", "1"), ("B", "2")])
        .await;
    harness.start_variable_set_reconciliation(&id).await;
    assert_eq!(
        harness.variable_set_phase(&id).await,
        ResourcePhase::Reconciling
    );
    assert_eq!(harness.variable_set_projection_entry_count(&id).await, 0);

    harness.settle().await;

    assert_eq!(harness.variable_set_phase(&id).await, ResourcePhase::Ready);
    assert_eq!(harness.variable_set_projection_entry_count(&id).await, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_secret_set_left_deleting_is_deleted_after_restart() {
    let harness = ControllersHarness::new().await;

    let id = harness
        .apply_secret_set(&harness.owner, "secrets", [("TOKEN", "value")])
        .await;
    assert_eq!(harness.secret_set_projection_entry_count(&id).await, 1);

    harness.request_secret_set_deletion_unsettled(id).await;
    assert_eq!(harness.secret_set_phase(&id).await, ResourcePhase::Deleting);
    assert_eq!(harness.secret_set_projection_entry_count(&id).await, 1);

    harness.settle().await;

    assert_eq!(harness.secret_set_phase(&id).await, ResourcePhase::Deleted);
    assert_eq!(harness.secret_set_projection_entry_count(&id).await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failing_secret_set_does_not_delay_variable_sets() {
    let harness = ControllersHarness::new().await;

    let failing_id = harness.create_secret_set_with_undecryptable_secret().await;
    let first_id = harness
        .apply_variable_set(&harness.owner, "first", [("A", "1")])
        .await;
    let second_id = harness
        .apply_variable_set(&harness.owner, "second", [("B", "2")])
        .await;

    assert_eq!(
        harness.variable_set_phase(&first_id).await,
        ResourcePhase::Ready
    );
    assert_eq!(
        harness.variable_set_phase(&second_id).await,
        ResourcePhase::Ready
    );

    // A transient failure, retried with a backoff, not again within the drains
    // that reconciled the others
    assert_eq!(
        harness.secret_set_phase(&failing_id).await,
        ResourcePhase::Failed
    );
    let state = harness.secret_set_controller_state(&failing_id).await;
    assert_eq!(state.attempts, 1);
    assert!(state.not_before.is_some(), "backing off: {state:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseConfigurationServiceHarness, base)]
struct ControllersHarness {
    base: BaseConfigurationServiceHarness,
    owner: odf::ResourceOwnerHandle,
}

impl ControllersHarness {
    async fn new() -> Self {
        let base = BaseConfigurationServiceHarness::new();
        let owner = base.ensure_test_account("test-owner").await;
        Self { base, owner }
    }

    /// Applies a `VariableSet` and leaves its reconciliation to a later drain
    async fn apply_variable_set_unsettled(
        &self,
        name: &str,
        variables: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        let spec = VariableSetSpecInput::new(odf::metadata::config::VariableSetSpecInput {
            variables: odf::metadata::config::Variables {
                entries: variables
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            odf::metadata::config::Variable {
                                value: value.to_string(),
                            },
                        )
                    })
                    .collect(),
            },
        });

        self.apply_variable_use_case()
            .apply(ApplyResourceParams {
                id: None,
                headers: BaseResourceServiceHarness::make_headers_input(self.owner.clone(), name),
                spec,
            })
            .await
            .unwrap()
            .expect_applied()
            .id
    }

    /// The first phase of a reconciliation, as a controller that crashed
    /// right after it would leave the resource
    async fn start_variable_set_reconciliation(&self, id: &ResourceID) {
        let primitives: Arc<dyn ResourceLifecyclePrimitives<VariableSetResource>> =
            self.catalog().get_one().unwrap();
        primitives
            .begin_reconciliation(id, None)
            .await
            .unwrap()
            .expect("the applied resource needs reconciliation");
    }

    /// Requests the deletion and leaves its completion to a later drain
    async fn request_secret_set_deletion_unsettled(&self, id: ResourceID) {
        let use_case: Arc<dyn RequestResourcesDeletionUseCase<SecretSetResource>> =
            self.catalog().get_one().unwrap();
        use_case
            .execute(self.owner.did.clone(), vec![id])
            .await
            .unwrap();
    }

    /// A `SecretSet` whose secret claims an encoding it does not have, written
    /// past the sanitizer, so that every reconciliation of it fails
    async fn create_secret_set_with_undecryptable_secret(&self) -> ResourceID {
        let spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
            secrets: odf::metadata::config::Secrets {
                entries: [(
                    "TOKEN".to_string(),
                    odf::metadata::config::Secret {
                        value: "not-a-jwe".to_string(),
                        content_encoding: Some("jwe".to_string()),
                    },
                )]
                .into(),
            },
        });

        let id = self.resource_repo().new_resource_id().await.unwrap();
        let headers =
            BaseResourceServiceHarness::make_headers_input(self.owner.clone(), "undecryptable");
        let event = SecretSetResource::make_created_event(Utc::now(), id, headers, spec);
        let mut resource = SecretSetResource::from_aggregate(Aggregate::new(id, event).unwrap());

        let persistence_service: Arc<dyn ResourcePersistenceService<SecretSetResource>> =
            self.catalog().get_one().unwrap();
        persistence_service.create(&mut resource).await.unwrap();

        id
    }

    async fn secret_set_controller_state(&self, id: &ResourceID) -> ResourceControllerState {
        self.catalog()
            .get_one::<dyn ResourceControllerStateRepository>()
            .unwrap()
            .get_state(&ResourceControllerName::new(SECRET_SET_CONTROLLER_NAME), id)
            .await
            .unwrap()
            .expect("the runtime keeps bookkeeping of every resource it processed")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
