// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::CatalogBuilder;
use event_sourcing::GetEventsOpts;
use kamu_accounts::Principal;
use kamu_core::{ApiKind, RequestOrigin};
use kamu_resources::{
    ApplyResourceOutcome,
    ApplyResourceUseCase,
    CreationOrigin,
    CreationOriginApi,
    CreationProvenance,
    ResourceEvent,
    ResourceID,
};
use kamu_resources_services::request_creation_provenance;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    TestResource,
    TestResourceEventStore,
    TestResourceSpec,
    register_test_resource_resource_service_layer,
};
use pretty_assertions::assert_eq;
use tokio_stream::StreamExt;

use crate::tests::utils::make_resource_params;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_records_provenance_in_created_event() {
    let cases = [
        (
            Principal::new_test_with(&"alice"),
            RequestOrigin::Api(ApiKind::GraphQl),
            CreationOrigin::Api {
                api: CreationOriginApi::GraphQl,
            },
        ),
        (
            Principal::new_test_with(&"alice"),
            RequestOrigin::Cli,
            CreationOrigin::Cli,
        ),
        (
            Principal::Node,
            RequestOrigin::Internal {
                producer: "dev.kamu.test.Producer".to_string(),
                detail: Some("flow-1".to_string()),
            },
            CreationOrigin::Internal {
                producer: "dev.kamu.test.Producer".to_string(),
                detail: Some("flow-1".to_string()),
            },
        ),
    ];

    for (principal, request_origin, expected_origin) in cases {
        let harness = CreationProvenanceHarness::new();

        let id = harness.create("res-a", &principal, &request_origin).await;

        let expected = CreationProvenance {
            principal: principal.handle().unwrap(),
            origin: expected_origin,
        };
        let events = harness.events(id).await;
        assert_eq!(events.len(), 1, "no status event follows creation");
        assert_eq!(
            CreationProvenanceHarness::created_event_provenance(&events[0]),
            Some(expected.clone())
        );
        assert_eq!(harness.stored_provenance(id).await, Some(expected));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_keeps_provenance_of_creation() {
    let harness = CreationProvenanceHarness::new();
    let creator = Principal::new_test_with(&"alice");
    let id = harness.create("res-a", &creator, &RequestOrigin::Cli).await;

    let outcome = harness
        .apply(
            "res-a",
            "v2",
            &Principal::Node,
            &RequestOrigin::Api(ApiKind::Rest),
        )
        .await;

    assert_eq!(outcome, ApplyResourceOutcome::Updated);
    assert_eq!(
        harness.stored_provenance(id).await,
        Some(CreationProvenance {
            principal: creator.handle().unwrap(),
            origin: CreationOrigin::Cli,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
struct CreationProvenanceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
    apply_svc: Arc<dyn ApplyResourceUseCase<TestResource>>,
}

impl CreationProvenanceHarness {
    fn new() -> Self {
        let base = BaseResourceServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        let catalog = b.build();

        Self {
            base,
            apply_svc: catalog.get_one().unwrap(),
            catalog,
        }
    }

    async fn create(
        &self,
        name: &str,
        principal: &Principal,
        request_origin: &RequestOrigin,
    ) -> ResourceID {
        let mut params = make_resource_params(odf::ResourceOwnerHandle::new_test("test"), name);
        params.provenance = request_creation_provenance(principal, request_origin).unwrap();

        let result = self.apply_svc.apply(params).await.unwrap().expect_applied();
        assert_eq!(result.outcome, ApplyResourceOutcome::Created);
        result.id
    }

    async fn apply(
        &self,
        name: &str,
        value: &str,
        principal: &Principal,
        request_origin: &RequestOrigin,
    ) -> ApplyResourceOutcome {
        let mut params = make_resource_params(odf::ResourceOwnerHandle::new_test("test"), name);
        params.spec.value = value.to_string();
        params.provenance = request_creation_provenance(principal, request_origin).unwrap();

        self.apply_svc
            .apply(params)
            .await
            .unwrap()
            .expect_applied()
            .outcome
    }

    async fn events(&self, id: ResourceID) -> Vec<ResourceEvent<TestResourceSpec>> {
        let event_store: Arc<dyn TestResourceEventStore> = self.catalog.get_one().unwrap();
        event_store
            .get_events(&id, GetEventsOpts::default())
            .map(|result| result.unwrap().1)
            .collect()
            .await
    }

    fn created_event_provenance(
        event: &ResourceEvent<TestResourceSpec>,
    ) -> Option<CreationProvenance> {
        let ResourceEvent::Created(created) = event else {
            panic!("unexpected first event: {event:?}");
        };
        CreationProvenance::from_conditions(created.initial_conditions.as_ref()?).unwrap()
    }

    async fn stored_provenance(&self, id: ResourceID) -> Option<CreationProvenance> {
        let snapshot = self.get_snapshot_by_id(&id).await.unwrap();
        CreationProvenance::from_conditions(&snapshot.basic_status().unwrap().conditions).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
