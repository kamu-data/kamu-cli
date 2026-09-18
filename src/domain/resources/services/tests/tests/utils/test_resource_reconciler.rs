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

use kamu_resources::Reconciler;

use crate::tests::utils::{TestResource, TestResourceReconcileError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn Reconciler<TestResource>)]
pub struct TestResourceReconciler;

#[async_trait::async_trait]
impl Reconciler<TestResource> for TestResourceReconciler {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceFailingReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn Reconciler<TestResource>)]
pub struct TestResourceFailingReconciler;

#[async_trait::async_trait]
impl Reconciler<TestResource> for TestResourceFailingReconciler {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        use internal_error::ErrorIntoInternal;
        Err(TestResourceReconcileError::Internal(
            "simulated reconcile failure".int_err(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceCountingReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Not a #[dill::component] — always registered via add_value() so that the
// Arc<AtomicU32> owned by the test is shared across all catalog resolutions.
pub struct TestResourceCountingReconciler {
    pub call_count: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Reconciler<TestResource> for TestResourceCountingReconciler {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum TestResourceReconcilerProvider {
    #[default]
    Stub,
    Failing,
    Counting(Arc<AtomicU32>),
}

impl TestResourceReconcilerProvider {
    pub fn embed_into_catalog(self, b: &mut dill::CatalogBuilder) {
        match self {
            TestResourceReconcilerProvider::Stub => {
                b.add::<TestResourceReconciler>();
            }
            TestResourceReconcilerProvider::Failing => {
                b.add::<TestResourceFailingReconciler>();
            }
            TestResourceReconcilerProvider::Counting(counter) => {
                b.add_value(TestResourceCountingReconciler {
                    call_count: counter,
                })
                .bind::<dyn Reconciler<TestResource>, TestResourceCountingReconciler>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
