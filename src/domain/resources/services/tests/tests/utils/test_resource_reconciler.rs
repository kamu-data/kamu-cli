// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::tests::utils::{TestResource, TestResourceReconcileError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn kamu_resources::Reconciler<TestResource>)]
pub struct TestResourceReconciler;

#[async_trait::async_trait]
impl kamu_resources::Reconciler<TestResource> for TestResourceReconciler {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceFailingReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn kamu_resources::Reconciler<TestResource>)]
pub struct TestResourceFailingReconciler;

#[async_trait::async_trait]
impl kamu_resources::Reconciler<TestResource> for TestResourceFailingReconciler {
    async fn reconcile(&self, _resource: &TestResource) -> Result<(), TestResourceReconcileError> {
        use internal_error::ErrorIntoInternal;
        Err(TestResourceReconcileError::Internal(
            "simulated reconcile failure".int_err(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum TestResourceReconcilerProvider {
    #[default]
    Stub,
    Failing,
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
