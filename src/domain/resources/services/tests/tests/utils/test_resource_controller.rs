// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use internal_error::ErrorIntoInternal;
use kamu_resources::{
    DeclarativeResource,
    ManagedResource,
    ResourceAggregateLoader,
    ResourceCleanupError,
    ResourceController,
    ResourcePersistenceService,
};

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

    async fn cleanup(&self, _resource: &TestResource) -> Result<(), ResourceCleanupError> {
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
        Err(TestResourceReconcileError::Internal(
            "simulated reconcile failure".int_err(),
        ))
    }

    async fn cleanup(&self, _resource: &TestResource) -> Result<(), ResourceCleanupError> {
        Err(ResourceCleanupError::Internal(
            "simulated cleanup failure".int_err(),
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

    async fn cleanup(&self, _resource: &TestResource) -> Result<(), ResourceCleanupError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceSelectiveCleanupController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fails the cleanup of the resource named `failing_name` only.
pub struct TestResourceSelectiveCleanupController {
    pub failing_name: &'static str,
}

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceSelectiveCleanupController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }

    async fn cleanup(&self, resource: &TestResource) -> Result<(), ResourceCleanupError> {
        if resource.headers().name.as_str() == self.failing_name {
            return Err(ResourceCleanupError::Internal(
                "simulated cleanup failure".int_err(),
            ));
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceInterferingCleanupController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// During its first cleanup, plays another writer that updates the status of
/// the resource being cleaned up, so the batch it belongs to conflicts.
#[dill::component(pub)]
#[dill::interface(dyn ResourceController<TestResource>)]
pub struct TestResourceInterferingCleanupController {
    catalog: dill::Catalog,
    interference: Arc<TestCleanupInterference>,
}

#[derive(Default)]
pub struct TestCleanupInterference {
    done: AtomicBool,
}

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceInterferingCleanupController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }

    async fn cleanup(&self, resource: &TestResource) -> Result<(), ResourceCleanupError> {
        if self.interference.done.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let loader = self
            .catalog
            .get_one::<dyn ResourceAggregateLoader<TestResource>>()
            .unwrap();
        let persistence = self
            .catalog
            .get_one::<dyn ResourcePersistenceService<TestResource>>()
            .unwrap();

        let mut other = loader.load(resource.id()).await.unwrap();
        let mut new_status = other.status().clone();
        new_status.observed_at = Some(chrono::Utc::now());
        let status_generation = other.headers().status_generation;
        other
            .try_update_status(chrono::Utc::now(), status_generation, new_status)
            .unwrap();
        persistence.save(&mut other).await.unwrap();

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceFailingCleanupOnceController
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fails its first cleanup and succeeds on every later one.
#[dill::component(pub)]
#[dill::interface(dyn ResourceController<TestResource>)]
pub struct TestResourceFailingCleanupOnceController {
    failure: Arc<TestCleanupFailureOnce>,
}

#[derive(Default)]
pub struct TestCleanupFailureOnce {
    done: AtomicBool,
}

#[async_trait::async_trait]
impl ResourceController<TestResource> for TestResourceFailingCleanupOnceController {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }

    async fn cleanup(&self, _resource: &TestResource) -> Result<(), ResourceCleanupError> {
        if self.failure.done.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        Err(ResourceCleanupError::Internal(
            "simulated cleanup failure".int_err(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum TestResourceControllerProvider {
    #[default]
    Stub,
    Failing,
    Counting(Arc<AtomicU32>),
    FailingCleanupOf(&'static str),
    InterferingCleanup,
    FailingCleanupOnce,
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
            TestResourceControllerProvider::FailingCleanupOf(failing_name) => {
                b.add_value(TestResourceSelectiveCleanupController { failing_name })
                    .bind::<dyn ResourceController<TestResource>, TestResourceSelectiveCleanupController>();
            }
            TestResourceControllerProvider::InterferingCleanup => {
                b.add_value(TestCleanupInterference::default());
                b.add::<TestResourceInterferingCleanupController>();
            }
            TestResourceControllerProvider::FailingCleanupOnce => {
                b.add_value(TestCleanupFailureOnce::default());
                b.add::<TestResourceFailingCleanupOnceController>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
