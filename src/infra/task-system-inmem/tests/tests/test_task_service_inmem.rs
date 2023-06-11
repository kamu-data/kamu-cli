// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_core::*;
use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;
use opendatafabric::*;

mockall::mock! {
    PullService {}
    #[async_trait::async_trait]
    impl PullService for PullService {
        async fn pull(
            &self,
            dataset_ref: &DatasetRefAny,
            options: PullOptions,
            listener: Option<Arc<dyn PullListener>>,
        ) -> Result<PullResult, PullError>;

        async fn pull_ext(
            &self,
            request: &PullRequest,
            options: PullOptions,
            listener: Option<Arc<dyn PullListener>>,
        ) -> Result<PullResult, PullError>;

        async fn pull_multi(
            &self,
            dataset_refs: Vec<DatasetRefAny>,
            options: PullMultiOptions,
            listener: Option<Arc<dyn PullMultiListener>>,
        ) -> Result<Vec<PullResponse>, InternalError>;

        async fn pull_multi_ext(
            &self,
            requests: Vec<PullRequest>,
            options: PullMultiOptions,
            listener: Option<Arc<dyn PullMultiListener>>,
        ) -> Result<Vec<PullResponse>, InternalError>;

        async fn set_watermark(
            &self,
            dataset_ref: &DatasetRef,
            watermark: DateTime<Utc>,
        ) -> Result<PullResult, SetWatermarkError>;
    }
}

#[test_log::test(tokio::test)]
async fn test_create_task() {
    let event_store = Arc::new(TaskEventStoreInMemory::new());
    let task_svc = TaskServiceInMemory::new(event_store, Arc::new(MockPullService::new()));

    let logical_plan_expected: LogicalPlan = Probe { ..Probe::default() }.into();

    let task_state_actual = task_svc
        .create_task(logical_plan_expected.clone())
        .await
        .unwrap();

    assert_matches!(task_state_actual, TaskState {
        task_id: _,
        status: TaskStatus::Queued,
        cancellation_requested: false,
        logical_plan,
        created_at: _,
        ran_at: None,
        cancellation_requested_at: None,
        finished_at: None,
    } if logical_plan == logical_plan_expected);
}
