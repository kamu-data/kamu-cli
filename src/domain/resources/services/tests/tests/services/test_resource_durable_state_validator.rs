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
use dill::CatalogBuilder;
use event_sourcing::Aggregate;
use internal_error::InternalError;
use kamu_resources::{
    DeclarativeResource,
    RESOURCE_CONDITION_READY_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    ReconcilableResource,
    ResourceAggregateLoader,
    ResourceAnnotations,
    ResourceConditionValue,
    ResourceDurableStateValidationError,
    ResourceExtensionKind,
    ResourceExtensionResolutionError,
    ResourceLabels,
    ResourcePersistenceError,
    ResourcePersistenceService,
    TypeRef,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{
    TestResource,
    TestResourceReconcileError,
    TestResourceReconciler,
    TestResourceSpec,
    TestResourceState,
    make_fresh_aggregate,
    make_id,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciliation_conditions_pass_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    harness
        .save_with_reconciliation_started_condition("res-started")
        .await
        .unwrap();
    harness
        .save_with_reconciliation_succeeded_conditions("res-ready")
        .await
        .unwrap();
    harness
        .save_with_reconciliation_failed_conditions("res-failed")
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_registered_label_short_name_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .create_with_labels(
            "res-label-alias",
            vec![("environment".parse().unwrap(), serde_json::json!("prod"))],
        )
        .await;

    harness.assert_durable_state_error(
        result,
        &ResourceDurableStateValidationError::NonCanonicalExtensionKey {
            kind: ResourceExtensionKind::Label,
            key: "environment".parse().unwrap(),
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_registered_annotation_short_name_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .create_with_annotations(
            "res-annotation-alias",
            vec![("description".parse().unwrap(), serde_json::json!("desc"))],
        )
        .await;

    harness.assert_durable_state_error(
        result,
        &ResourceDurableStateValidationError::NonCanonicalExtensionKey {
            kind: ResourceExtensionKind::Annotation,
            key: "description".parse().unwrap(),
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unknown_label_uri_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .create_with_labels(
            "res-unknown-label-uri",
            vec![(
                "https://example.com/schemas/resource/labels/Unknown"
                    .parse()
                    .unwrap(),
                serde_json::json!("prod"),
            )],
        )
        .await;

    harness.assert_durable_state_error(
        result,
        &ResourceDurableStateValidationError::ExtensionResolution(
            ResourceExtensionResolutionError::UnknownUri {
                kind: ResourceExtensionKind::Label,
                uri: "https://example.com/schemas/resource/labels/Unknown"
                    .parse()
                    .unwrap(),
            },
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_registered_label_uri_value_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .create_with_labels(
            "res-invalid-label-value",
            vec![(
                RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI.parse().unwrap(),
                serde_json::json!({ "not": "a string" }),
            )],
        )
        .await;

    harness.assert_invalid_extension_value_error(
        result,
        ResourceExtensionKind::Label,
        &RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI.parse().unwrap(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unknown_free_form_label_short_name_passes_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    harness
        .create_with_labels(
            "res-free-form-label",
            vec![(
                "env".parse().unwrap(),
                serde_json::json!({ "nested": true }),
            )],
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_short_name_condition_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .save_with_condition(
            "res-short-condition",
            "Ready".parse().unwrap(),
            ResourceDurableStateValidatorHarness::valid_condition_value(),
        )
        .await;

    harness.assert_durable_state_error(
        result,
        &ResourceDurableStateValidationError::NonCanonicalExtensionKey {
            kind: ResourceExtensionKind::Condition,
            key: "Ready".parse().unwrap(),
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unknown_condition_uri_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .save_with_condition(
            "res-unknown-condition",
            "https://example.com/schemas/resource/conditions/Unknown"
                .parse()
                .unwrap(),
            ResourceDurableStateValidatorHarness::valid_condition_value(),
        )
        .await;

    harness.assert_durable_state_error(
        result,
        &ResourceDurableStateValidationError::ExtensionResolution(
            ResourceExtensionResolutionError::UnknownUri {
                kind: ResourceExtensionKind::Condition,
                uri: "https://example.com/schemas/resource/conditions/Unknown"
                    .parse()
                    .unwrap(),
            },
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_malformed_condition_value_fails_durable_state_guard() {
    let harness = ResourceDurableStateValidatorHarness::new();

    let result = harness
        .save_with_condition(
            "res-malformed-condition",
            RESOURCE_CONDITION_READY_SCHEMA_URI.parse().unwrap(),
            serde_json::json!({ "value": "True" }),
        )
        .await;

    harness.assert_invalid_extension_value_error(
        result,
        ResourceExtensionKind::Condition,
        &RESOURCE_CONDITION_READY_SCHEMA_URI.parse().unwrap(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct ResourceDurableStateValidatorHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
    account_handle: odf::AccountHandle,
}

impl ResourceDurableStateValidatorHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        b.add::<TestResourceReconciler>();

        Self {
            base,
            catalog: b.build(),
            account_handle: odf::AccountHandle::new_test("test"),
        }
    }

    fn persistence_svc(&self) -> Arc<dyn ResourcePersistenceService<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    fn aggregate_loader(&self) -> Arc<dyn ResourceAggregateLoader<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    fn assert_durable_state_error(
        &self,
        result: Result<(), ResourcePersistenceError>,
        expected: &ResourceDurableStateValidationError,
    ) {
        pretty_assertions::assert_matches!(
            result,
            Err(ResourcePersistenceError::InvalidDurableState(err)) if err == *expected
        );
    }

    fn assert_invalid_extension_value_error(
        &self,
        result: Result<(), ResourcePersistenceError>,
        expected_kind: ResourceExtensionKind,
        expected_key: &TypeRef,
    ) {
        pretty_assertions::assert_matches!(
            result,
            Err(ResourcePersistenceError::InvalidDurableState(
                ResourceDurableStateValidationError::InvalidExtensionValue { kind, key, .. }
            )) if kind == expected_kind && key == *expected_key
        );
    }

    fn valid_condition_value() -> serde_json::Value {
        serde_json::to_value(ResourceConditionValue::ready_true(Utc::now()).1).unwrap()
    }

    async fn create_with_labels(
        &self,
        name: &str,
        labels: Vec<(TypeRef, serde_json::Value)>,
    ) -> Result<(), ResourcePersistenceError> {
        let mut agg = self.make_fresh_aggregate_with_metadata(name, labels, Vec::new());
        self.create_and_revert_on_error(&mut agg).await
    }

    async fn create_with_annotations(
        &self,
        name: &str,
        annotations: Vec<(TypeRef, serde_json::Value)>,
    ) -> Result<(), ResourcePersistenceError> {
        let mut agg = self.make_fresh_aggregate_with_metadata(name, Vec::new(), annotations);
        self.create_and_revert_on_error(&mut agg).await
    }

    async fn save_with_reconciliation_started_condition(
        &self,
        name: &str,
    ) -> Result<(), ResourcePersistenceError> {
        let mut agg = self.create_and_load(name).await;
        agg.try_mark_reconciliation_started(Utc::now()).unwrap();
        self.persistence_svc().save(&mut agg).await
    }

    async fn save_with_reconciliation_succeeded_conditions(
        &self,
        name: &str,
    ) -> Result<(), ResourcePersistenceError> {
        let mut agg = self.create_and_load(name).await;
        agg.try_mark_reconciliation_succeeded(Utc::now(), agg.headers().generation, ())
            .unwrap();
        self.persistence_svc().save(&mut agg).await
    }

    async fn save_with_reconciliation_failed_conditions(
        &self,
        name: &str,
    ) -> Result<(), ResourcePersistenceError> {
        let mut agg = self.create_and_load(name).await;
        let err = TestResourceReconcileError::Internal(InternalError::new("test failure"));
        agg.try_mark_reconciliation_failed(Utc::now(), agg.headers().generation, &err)
            .unwrap();
        self.persistence_svc().save(&mut agg).await
    }

    async fn save_with_condition(
        &self,
        name: &str,
        key: TypeRef,
        value: serde_json::Value,
    ) -> Result<(), ResourcePersistenceError> {
        let agg = self.create_and_load(name).await;
        let mut snapshot =
            <TestResource as kamu_resources::ReconcilableEventSourcedResource>::make_resource_snapshot(
                &agg,
            )
            .unwrap();
        snapshot
            .status
            .as_mut()
            .unwrap()
            .conditions
            .entries
            .insert(key, value);
        let state = TestResourceState::try_from(snapshot.clone()).unwrap();
        let last_event_id = snapshot.last_event_id.unwrap();
        let mut corrupted = TestResource(Aggregate::from_stored_snapshot(
            snapshot.id,
            last_event_id,
            state,
        ));

        self.persistence_svc().save(&mut corrupted).await
    }

    async fn create_and_load(&self, name: &str) -> TestResource {
        let (id, mut agg) = make_fresh_aggregate(self.account_handle.clone(), name);
        self.persistence_svc().create(&mut agg).await.unwrap();
        self.aggregate_loader().load(&id).await.unwrap()
    }

    async fn create_and_revert_on_error(
        &self,
        agg: &mut TestResource,
    ) -> Result<(), ResourcePersistenceError> {
        let result = self.persistence_svc().create(agg).await;
        if result.is_err() {
            kamu_resources::ReconcilableEventSourcedResource::revert(agg);
        }
        result
    }

    fn make_fresh_aggregate_with_metadata(
        &self,
        name: &str,
        labels: Vec<(TypeRef, serde_json::Value)>,
        annotations: Vec<(TypeRef, serde_json::Value)>,
    ) -> TestResource {
        let id = make_id();
        let mut headers =
            BaseResourceServiceHarness::make_headers_input(self.account_handle.clone(), name);
        headers.labels = Some(ResourceLabels {
            entries: labels.into_iter().collect(),
        });
        headers.annotations = Some(ResourceAnnotations {
            entries: annotations.into_iter().collect(),
        });

        TestResource::try_create(
            Utc::now(),
            id,
            headers,
            TestResourceSpec {
                value: name.to_string(),
            },
        )
        .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
