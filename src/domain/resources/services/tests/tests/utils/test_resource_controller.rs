// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kamu_resources::ResourceController;

use crate::tests::utils::{TestResource, TestResourceReconcileError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceController<TestResource>)]
pub struct TestResourceController;

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceFailingController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceController<TestResource>)]
pub struct TestResourceFailingController;

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceFailingController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        use internal_error::ErrorIntoInternal;
        Err(TestResourceReconcileError::Internal(
            "simulated reconcile failure".int_err(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceCountingController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Not a #[dill::component] — always registered via add_value() so that the
// Arc<AtomicU32> owned by the test is shared across all catalog resolutions.
pub struct TestResourceCountingController {
    pub call_count: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceCountingController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum TestResourceControllerProvider {
    #[default]
    Stub,
    Failing,
    Counting(Arc<AtomicU32>),
}

impl TestResourceControllerProvider {
    pub fn embed_into_catalog(self, b: &mut dill::CatalogBuilder) {
        match self {
            TestResourceControllerProvider::Stub => {
                b.add::<TestResourceController>();
            }
            TestResourceControllerProvider::Failing => {
                b.add::<TestResourceFailingController>();
            }
            TestResourceControllerProvider::Counting(counter) => {
                b.add_value(TestResourceCountingController {
                    call_count: counter,
                })
                .bind::<dyn ResourceController<TestResource>, TestResourceCountingController>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
