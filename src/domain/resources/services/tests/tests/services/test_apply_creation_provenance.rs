// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::Principal;
use kamu_core::{ApiKind, RequestOrigin};
use kamu_resources::{
    ApplyResourceOutcome,
    ApplyResourceUseCase,
    CreationOrigin,
    CreationOriginApi,
    CreationProvenance,
    ResourceID,
};
use kamu_resources_services::request_creation_provenance;
use kamu_resources_services::testing::TestResource;
use pretty_assertions::assert_eq;

use crate::tests::utils::{ServiceLayerTestHarness, make_resource_params};

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
        assert_eq!(
            harness.events(id).await.len(),
            1,
            "no status event follows creation"
        );
        assert_eq!(
            harness.created_event_provenance(id).await,
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

#[oop::extend(ServiceLayerTestHarness, base)]
struct CreationProvenanceHarness {
    base: ServiceLayerTestHarness,
    apply_svc: Arc<dyn ApplyResourceUseCase<TestResource>>,
}

impl CreationProvenanceHarness {
    fn new() -> Self {
        let base = ServiceLayerTestHarness::new();

        Self {
            apply_svc: base.apply_test_uc(),
            base,
        }
    }

    /// The account of `alice`, who may apply there, as the node may
    fn account() -> odf::ResourceOwnerHandle {
        let Principal::User(alice) = Principal::new_test_with(&"alice") else {
            unreachable!()
        };
        (&alice).into()
    }

    async fn create(
        &self,
        name: &str,
        principal: &Principal,
        request_origin: &RequestOrigin,
    ) -> ResourceID {
        let mut params = make_resource_params(Self::account(), name);
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
        let mut params = make_resource_params(Self::account(), name);
        params.spec.value = value.to_string();
        params.provenance = request_creation_provenance(principal, request_origin).unwrap();

        self.apply_svc
            .apply(params)
            .await
            .unwrap()
            .expect_applied()
            .outcome
    }

    async fn created_event_provenance(&self, id: ResourceID) -> Option<CreationProvenance> {
        let created = self.created_event(id).await;
        CreationProvenance::from_conditions(created.initial_conditions.as_ref()?).unwrap()
    }

    async fn stored_provenance(&self, id: ResourceID) -> Option<CreationProvenance> {
        let snapshot = self.get_snapshot_by_id(&id).await.unwrap();
        CreationProvenance::from_conditions(&snapshot.basic_status().unwrap().conditions).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
