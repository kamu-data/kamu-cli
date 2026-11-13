// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::{Component, TypedBuilderExt};
use internal_error::InternalError;
use kamu_core::*;

use crate::DatasetUpdateStepsImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the steps of `DatasetUpdateStepsImpl`, but when armed first deletes
/// the dataset before the next `commit_head`, as if it was deleted while the
/// update ran. [`Self::register`] adds it with the steps it wraps.
pub struct RacingDatasetUpdateSteps {
    steps: Arc<DatasetUpdateStepsImpl>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    delete_before_next_commit: Mutex<bool>,
}

#[dill::component(pub)]
#[dill::interface(dyn DatasetUpdateSteps)]
#[dill::scope(dill::Singleton)]
impl RacingDatasetUpdateSteps {
    pub fn new(
        steps: Arc<DatasetUpdateStepsImpl>,
        dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    ) -> Self {
        Self {
            steps,
            dataset_storage_unit_writer,
            delete_before_next_commit: Mutex::new(false),
        }
    }
}

impl RacingDatasetUpdateSteps {
    /// Registers this as the only `DatasetUpdateSteps`: the wrapped steps are
    /// registered without their interface
    pub fn register(b: &mut dill::CatalogBuilder) {
        b.add_builder(DatasetUpdateStepsImpl::builder().without_default_interfaces())
            .add::<Self>();
    }

    /// Arms the next `commit_head`, or `transfer_and_commit_sync` into an
    /// existing dataset, to delete that dataset first
    pub fn delete_dataset_before_next_commit(&self) {
        *self.delete_before_next_commit.lock().unwrap() = true;
    }
}

#[async_trait::async_trait]
impl DatasetUpdateSteps for RacingDatasetUpdateSteps {
    async fn plan_ingest(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<PlannedIngest>, PollingIngestPlanError> {
        self.steps.plan_ingest(target).await
    }

    async fn elaborate_transform(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<ElaboratedTransform>, TransformElaborateError> {
        self.steps.elaborate_transform(target).await
    }

    async fn resolve_sync_target(
        &self,
        target: &PullLocalTarget,
    ) -> Result<Option<SyncFromTarget>, SyncError> {
        self.steps.resolve_sync_target(target).await
    }

    async fn commit_head(
        &self,
        target: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<HeadCommit, InternalError> {
        if std::mem::take(&mut *self.delete_before_next_commit.lock().unwrap()) {
            self.dataset_storage_unit_writer
                .delete_dataset(&target.id)
                .await
                .unwrap();
        }

        self.steps.commit_head(target, old_head, new_head).await
    }

    async fn commit_sync(
        &self,
        transferred: SyncFromTransferred,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        self.steps.commit_sync(transferred, maybe_pull_alias).await
    }

    async fn transfer_and_commit_sync(
        &self,
        request: SyncFromRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        if std::mem::take(&mut *self.delete_before_next_commit.lock().unwrap())
            && let SyncFromTarget::Existing { dataset, .. } = &request.dst
        {
            self.dataset_storage_unit_writer
                .delete_dataset(dataset.get_id())
                .await
                .unwrap();
        }

        self.steps
            .transfer_and_commit_sync(request, options, listener, maybe_pull_alias)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
