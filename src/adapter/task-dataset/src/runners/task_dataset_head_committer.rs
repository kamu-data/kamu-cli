// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::{DatasetHeadCommitter, DatasetRegistry};
use kamu_task_system::TaskOutcome;

use crate::{DatasetHeadMovedError, TaskErrorDatasetCommit};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits a `HEAD` proposed by a task, translating a concurrent `HEAD` move
/// into the task's failed outcome.
pub(crate) struct TaskDatasetHeadCommitter<'a> {
    catalog: &'a dill::Catalog,
}

impl<'a> TaskDatasetHeadCommitter<'a> {
    pub(crate) fn new(catalog: &'a dill::Catalog) -> Self {
        Self { catalog }
    }

    /// Moves `HEAD` from `old_head` to `new_head` in a transaction of its own.
    /// Returns the failed outcome when `HEAD` was moved concurrently.
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, %new_head))]
    pub(crate) async fn commit(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<Option<TaskOutcome>, InternalError> {
        match self
            .commit_transactionally(dataset_handle, old_head, new_head)
            .await
        {
            Ok(()) => Ok(None),
            Err(odf::dataset::SetChainRefError::CASFailed(e)) => {
                Ok(Some(Self::head_moved_outcome(dataset_handle.id.clone(), e)))
            }
            Err(
                e @ (odf::dataset::SetChainRefError::BlockNotFound(_)
                | odf::dataset::SetChainRefError::Access(_)
                | odf::dataset::SetChainRefError::Internal(_)),
            ) => Err(e.int_err()),
        }
    }

    pub(crate) fn head_moved_outcome(
        dataset_id: odf::DatasetID,
        cas_error: odf::dataset::RefCASError,
    ) -> TaskOutcome {
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError::new(dataset_id, cas_error))
                .into_task_error(),
        )
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_transactionally(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        dataset_head_committer
            .commit_head(&target, Some(old_head), new_head)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
