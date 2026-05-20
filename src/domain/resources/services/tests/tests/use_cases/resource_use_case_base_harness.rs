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
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceParams,
    ApplyResourceUseCase,
    DeleteAccountResourcesUseCase,
    DeleteResourcesUseCase,
    ReconcileResourceUseCase,
    ResourceUID,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::{MockOutbox, OutboxAgent, OutboxProvider};

use crate::tests::utils::{
    TestResource,
    TestResourceReconcilerProvider,
    TestResourceSpec,
    TestResourceSpecSanitizer,
    register_test_resource_crud_dispatcher,
    register_test_resource_resource_service_layer,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceUseCaseBaseHarnessOpts {
    pub base_opts: BaseResourceServiceHarnessOpts,
    pub reconciler_provider: TestResourceReconcilerProvider,
    pub with_sanitizer: bool,
}

impl Default for ResourceUseCaseBaseHarnessOpts {
    fn default() -> Self {
        Self {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Immediate {
                    force_immediate: true,
                },
            },
            reconciler_provider: TestResourceReconcilerProvider::default(),
            with_sanitizer: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseResourceServiceHarness, base)]
pub struct ResourceUseCaseBaseHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl ResourceUseCaseBaseHarness {
    pub fn new() -> Self {
        Self::new_with_opts(ResourceUseCaseBaseHarnessOpts::default())
    }

    pub fn new_with_mock_outbox(mock_outbox: MockOutbox) -> Self {
        Self::new_with_opts(ResourceUseCaseBaseHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Mock(mock_outbox),
            },
            ..Default::default()
        })
    }

    pub fn new_with_opts(opts: ResourceUseCaseBaseHarnessOpts) -> Self {
        let base = BaseResourceServiceHarness::new_with_opts(opts.base_opts);

        let mut b = CatalogBuilder::new_chained(base.catalog());
        register_test_resource_resource_service_layer(&mut b);
        register_test_resource_crud_dispatcher(&mut b);
        opts.reconciler_provider.embed_into_catalog(&mut b);

        if opts.with_sanitizer {
            b.add_value(TestResourceSpecSanitizer {
                suffix: "-sanitized".to_string(),
            })
            .bind::<dyn kamu_resources::ResourceSpecSanitizer<TestResource>, TestResourceSpecSanitizer>();
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

    pub fn delete_test_uc(&self) -> Arc<dyn DeleteResourcesUseCase<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn delete_account_resources_uc(&self) -> Arc<dyn DeleteAccountResourcesUseCase> {
        self.catalog.get_one().unwrap()
    }

    pub fn reconcile_test_uc(&self) -> Arc<dyn ReconcileResourceUseCase<TestResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn outbox_agent(&self) -> Arc<dyn OutboxAgent> {
        self.catalog.get_one().unwrap()
    }

    pub async fn delete_resources(&self, account_id: odf::AccountID, uids: Vec<ResourceUID>) {
        self.delete_test_uc()
            .execute(account_id, uids)
            .await
            .unwrap();
    }

    pub async fn delete_account_resources(&self, account_id: odf::AccountID) {
        self.delete_account_resources_uc()
            .execute(account_id)
            .await
            .unwrap();
    }

    pub async fn apply_and_assert_snapshot(
        &self,
        account_id: odf::AccountID,
        name: &str,
    ) -> (ResourceUID, kamu_resources::ResourceSnapshot) {
        let uid = self.apply_and_get_uid(account_id, name).await;
        let snapshot = self.get_snapshot_by_uid(&uid).await.unwrap();
        (uid, snapshot)
    }

    pub async fn apply_and_get_uid(&self, account_id: odf::AccountID, name: &str) -> ResourceUID {
        let params = ApplyResourceParams {
            uid: None,
            metadata: BaseResourceServiceHarness::make_metadata_input(account_id, name),
            spec: TestResourceSpec {
                value: name.to_string(),
            },
        };

        let decision = self.apply_test_uc().apply(params).await.unwrap();

        match decision {
            ApplyResourceApplicationDecision::Applied(result) => result.uid,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("apply rejected: {:?}", r.message)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
