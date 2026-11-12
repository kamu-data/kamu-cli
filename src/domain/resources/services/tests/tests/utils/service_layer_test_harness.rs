// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::TimeDelta;
use dill::CatalogBuilder;
use internal_error::InternalError;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourceUseCase,
    RequestAccountResourcesDeletionUseCase,
    RequestResourcesDeletionUseCase,
    ResourceController,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceProcessOutcome,
    ResourceSpecSanitizer,
};
use kamu_resources_services::BackgroundWorkFinalizer;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::{MockOutbox, OutboxAgent, OutboxProvider};
use time_source::FakeSystemTimeSource;

use crate::tests::utils::{
    TEST_RESOURCE_SIMPLE_CONTROLLER_NAME,
    TestResource,
    TestResourceReconcilerProvider,
    TestResourceSpec,
    TestResourceSpecClearingSanitizer,
    TestResourceSpecSanitizer,
    register_test_condition_schema_dispatchers,
    register_test_resource_crud_dispatcher,
    register_test_resource_resource_service_layer,
    register_test_resource_resource_service_layer_without_controller,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum SanitizerKind {
    #[default]
    None,
    Appending,
    Clearing,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ServiceLayerTestHarnessOpts {
    pub base_opts: BaseResourceServiceHarnessOpts,
    pub reconciler_provider: TestResourceReconcilerProvider,
    pub sanitizer: SanitizerKind,
    /// Whether the controller runtime drives the test type through its simple
    /// controller adapter, which `settle` drains. Without it, the test brings
    /// its own controller and runtime.
    pub simple_controller: bool,
}

impl Default for ServiceLayerTestHarnessOpts {
    fn default() -> Self {
        Self {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Immediate {
                    force_immediate: true,
                },
                ..Default::default()
            },
            reconciler_provider: TestResourceReconcilerProvider::default(),
            sanitizer: SanitizerKind::None,
            simple_controller: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
pub struct ServiceLayerTestHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl ServiceLayerTestHarness {
    pub fn new() -> Self {
        Self::new_with_opts(ServiceLayerTestHarnessOpts::default())
    }

    pub fn new_with_mock_outbox(mock_outbox: MockOutbox) -> Self {
        Self::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Mock(mock_outbox),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    pub fn new_with_mock_outbox_and_reconciler(
        mock_outbox: MockOutbox,
        reconciler_provider: TestResourceReconcilerProvider,
    ) -> Self {
        Self::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Mock(mock_outbox),
                ..Default::default()
            },
            reconciler_provider,
            ..Default::default()
        })
    }

    pub fn new_with_opts(opts: ServiceLayerTestHarnessOpts) -> Self {
        // The runtime drives the controllers of its own catalog, so it is
        // registered here, next to the adapter
        let base = BaseResourceServiceHarness::new_with_opts(BaseResourceServiceHarnessOpts {
            controller_runtime: false,
            ..opts.base_opts
        });

        let mut b = CatalogBuilder::new_chained(base.catalog());
        if opts.simple_controller {
            register_test_resource_resource_service_layer(&mut b);
            BaseResourceServiceHarness::register_controller_runtime(&mut b);
        } else {
            register_test_resource_resource_service_layer_without_controller(&mut b);
        }
        register_test_resource_crud_dispatcher(&mut b);
        register_test_condition_schema_dispatchers(&mut b);
        opts.reconciler_provider.embed_into_catalog(&mut b);

        match opts.sanitizer {
            SanitizerKind::None => {}
            SanitizerKind::Appending => {
                b.add_value(TestResourceSpecSanitizer {
                    suffix: "-sanitized".to_string(),
                })
                .bind::<dyn ResourceSpecSanitizer<TestResource>, TestResourceSpecSanitizer>();
            }
            SanitizerKind::Clearing => {
                b.add_value(TestResourceSpecClearingSanitizer)
                    .bind::<dyn ResourceSpecSanitizer<TestResource>, TestResourceSpecClearingSanitizer>();
            }
        }

        let catalog = b.build();

        Self { base, catalog }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn apply_test_uc(&self) -> Arc<dyn ApplyResourceUseCase<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn request_deletion_test_uc(
        &self,
    ) -> Arc<dyn RequestResourcesDeletionUseCase<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn request_account_resources_deletion_uc(
        &self,
    ) -> Arc<dyn RequestAccountResourcesDeletionUseCase> {
        self.catalog.get_one().unwrap()
    }

    pub fn outbox_agent(&self) -> Arc<dyn OutboxAgent> {
        self.catalog.get_one().unwrap()
    }

    pub async fn request_resources_deletion(
        &self,
        account_id: odf::AccountID,
        ids: Vec<ResourceID>,
    ) {
        self.request_deletion_test_uc()
            .execute(account_id, ids)
            .await
            .unwrap();
    }

    pub async fn request_account_resources_deletion(&self, account_id: odf::AccountID) {
        self.request_account_resources_deletion_uc()
            .execute(account_id)
            .await
            .unwrap();
    }

    /// See [`BackgroundWorkFinalizer`]
    pub async fn settle(&self) {
        BackgroundWorkFinalizer
            .finalize(&self.catalog)
            .await
            .unwrap();
    }

    /// One `process` call of the type's simple controller adapter
    pub async fn process(&self, id: &ResourceID) -> Result<ResourceProcessOutcome, InternalError> {
        self.catalog()
            .get_one::<dyn ResourceController>()
            .unwrap()
            .process(id)
            .await
    }

    /// The runtime's bookkeeping of the adapter for the resource
    pub async fn controller_state(&self, id: &ResourceID) -> ResourceControllerState {
        self.catalog()
            .get_one::<dyn ResourceControllerStateRepository>()
            .unwrap()
            .get_state(
                &ResourceControllerName::new(TEST_RESOURCE_SIMPLE_CONTROLLER_NAME),
                id,
            )
            .await
            .unwrap()
            .expect("the runtime keeps bookkeeping of every resource it processed")
    }

    /// Needs `fake_time`
    pub fn advance_time(&self, delta: TimeDelta) {
        self.catalog()
            .get_one::<FakeSystemTimeSource>()
            .unwrap()
            .advance(delta);
    }

    pub async fn apply_and_assert_snapshot(
        &self,
        account_handle: &odf::ResourceOwnerHandle,
        name: &str,
    ) -> (ResourceID, kamu_resources::ResourceSnapshot) {
        let id = self.apply_and_get_id(account_handle, name).await;
        let snapshot = self.get_snapshot_by_id(&id).await.unwrap();
        (id, snapshot)
    }

    pub async fn apply_and_get_id(
        &self,
        account_handle: &odf::ResourceOwnerHandle,
        name: &str,
    ) -> ResourceID {
        let params = ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_handle.clone(), name),
            spec: TestResourceSpec {
                value: name.to_string(),
            },
        };

        let decision = self.apply_test_uc().apply(params).await.unwrap();

        match decision {
            ApplyResourceApplicationDecision::Applied(result) => result.id,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("apply rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
