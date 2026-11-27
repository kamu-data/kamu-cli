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
use event_sourcing::GetEventsOpts;
use internal_error::InternalError;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourceUseCase,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ManagedResource,
    RequestAccountResourcesDeletionUseCase,
    RequestResourcesDeletionUseCase,
    ResourceAggregateLoader,
    ResourceController,
    ResourceControllerName,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceEvent,
    ResourceEventCreated,
    ResourceID,
    ResourceLifecycleMessage,
    ResourcePersistenceService,
    ResourceProcessOutcome,
    ResourceSpecPreApply,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
    TEST_RESOURCE_SIMPLE_CONTROLLER_NAME,
    TestResource,
    TestResourceAppendingPreApply,
    TestResourceClearingPreApply,
    TestResourceEventStore,
    TestResourceReconcilerProvider,
    TestResourceSpec,
    register_test_condition_schema_dispatchers,
    register_test_resource_crud_dispatcher,
    register_test_resource_resource_service_layer,
    register_test_resource_resource_service_layer_without_controller,
};
use messaging_outbox::{
    MockOutbox,
    OutboxAgent,
    OutboxMessageBoundary,
    OutboxMessageBridge,
    OutboxProvider,
};
use time_source::FakeSystemTimeSource;
use tokio_stream::StreamExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum SpecPreApplyKind {
    #[default]
    None,
    Appending,
    Clearing,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ServiceLayerTestHarnessOpts {
    pub base_opts: BaseResourceServiceHarnessOpts,
    pub reconciler_provider: TestResourceReconcilerProvider,
    pub spec_pre_apply: SpecPreApplyKind,
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
            spec_pre_apply: SpecPreApplyKind::None,
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

        match opts.spec_pre_apply {
            SpecPreApplyKind::None => {}
            SpecPreApplyKind::Appending => {
                b.add_value(TestResourceAppendingPreApply {
                    suffix: "-prepared".to_string(),
                })
                .bind::<dyn ResourceSpecPreApply<TestResource>, TestResourceAppendingPreApply>();
            }
            SpecPreApplyKind::Clearing => {
                b.add_value(TestResourceClearingPreApply)
                    .bind::<dyn ResourceSpecPreApply<TestResource>, TestResourceClearingPreApply>();
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

    /// See [`background_agent_host::BackgroundWorkFinalizer`]
    pub async fn settle(&self) {
        self.base
            .background_work_finalizer()
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

    /// Applies a new spec `value` to the existing resource `id`
    pub async fn update_spec(
        &self,
        account_handle: &odf::ResourceOwnerHandle,
        id: ResourceID,
        name: &str,
        value: &str,
    ) {
        let params = ApplyResourceParams {
            id: Some(id),
            ..BaseResourceServiceHarness::make_apply_params(
                account_handle.clone(),
                Some(name),
                TestResourceSpec {
                    value: value.to_string(),
                },
            )
        };

        self.apply_test_uc()
            .apply(params)
            .await
            .unwrap()
            .expect_applied();
    }

    /// Applies one resource per name, in order
    pub async fn apply_all(
        &self,
        account_handle: &odf::ResourceOwnerHandle,
        names: &[impl AsRef<str>],
    ) -> Vec<ResourceID> {
        let mut ids = Vec::with_capacity(names.len());
        for name in names {
            ids.push(self.apply_and_get_id(account_handle, name.as_ref()).await);
        }
        ids
    }

    pub async fn assert_deleted(&self, ids: &[ResourceID]) {
        for id in ids {
            assert!(self.is_deleted(id).await, "expected {id} to be deleted");
        }
    }

    pub async fn assert_live(&self, id: &ResourceID) {
        let snapshot = self.get_snapshot_by_id(id).await.unwrap();
        assert!(
            snapshot.headers.deletion_requested_at.is_none(),
            "expected {id} to be live"
        );
    }

    pub async fn load(&self, id: ResourceID) -> TestResource {
        self.catalog()
            .get_one::<dyn ResourceAggregateLoader<TestResource>>()
            .unwrap()
            .load(&id)
            .await
            .unwrap()
    }

    async fn save(&self, resource: &mut TestResource) {
        self.catalog()
            .get_one::<dyn ResourcePersistenceService<TestResource>>()
            .unwrap()
            .save(resource)
            .await
            .unwrap();
    }

    /// Stores the resource as being deleted through its aggregate, with no
    /// lifecycle message
    pub async fn request_deletion_of(&self, id: ResourceID) {
        let mut resource = self.load(id).await;
        resource
            .try_request_deletion(self.time_source().now())
            .unwrap();
        self.save(&mut resource).await;
    }

    /// Stores the resource being deleted as deleted through its aggregate,
    /// with no lifecycle message
    pub async fn complete_deletion_of(&self, id: ResourceID) {
        let mut resource = self.load(id).await;
        resource
            .try_complete_deletion(self.time_source().now())
            .unwrap();
        self.save(&mut resource).await;
    }

    pub async fn events(&self, id: ResourceID) -> Vec<ResourceEvent<TestResourceSpec>> {
        self.catalog()
            .get_one::<dyn TestResourceEventStore>()
            .unwrap()
            .get_events(&id, GetEventsOpts::default())
            .map(|result| result.unwrap().1)
            .collect()
            .await
    }

    /// The first event of the resource, which creates it
    pub async fn created_event(&self, id: ResourceID) -> ResourceEventCreated<TestResourceSpec> {
        match self.events(id).await.into_iter().next() {
            Some(ResourceEvent::Created(created)) => created,
            other => panic!("unexpected first event: {other:?}"),
        }
    }

    /// Needs `OutboxProvider::Dispatching`, whose messages stay in the outbox
    pub async fn lifecycle_messages(&self) -> Vec<ResourceLifecycleMessage> {
        self.raw_messages_since(0)
            .await
            .into_iter()
            .map(|content| serde_json::from_value(content).unwrap())
            .collect()
    }

    /// The JSON of every lifecycle message posted after the first `mark`, as
    /// the outbox stores it. Needs `OutboxProvider::Dispatching`
    pub async fn raw_messages_since(&self, mark: usize) -> Vec<serde_json::Value> {
        self.catalog()
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap()
            .get_messages_by_producer(
                self.catalog(),
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                OutboxMessageBoundary::default(),
                usize::MAX,
            )
            .await
            .unwrap()
            .into_iter()
            .skip(mark)
            .map(|message| message.content_json)
            .collect()
    }

    pub async fn apply_and_get_id(
        &self,
        account_handle: &odf::ResourceOwnerHandle,
        name: &str,
    ) -> ResourceID {
        let params = BaseResourceServiceHarness::make_apply_params(
            account_handle.clone(),
            Some(name),
            TestResourceSpec {
                value: name.to_string(),
            },
        );

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
