// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PushRequestPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
}

#[component(pub)]
#[interface(dyn PushRequestPlanner)]
impl PushRequestPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
    ) -> Self {
        Self {
            dataset_registry,
            remote_alias_resolver,
        }
    }

    async fn collect_push_plan_item(
        &self,
        local_handle: DatasetHandle,
        push_target: Option<&DatasetPushTarget>,
    ) -> Result<PushItem, PushResponse> {
        let local_target = self
            .dataset_registry
            .get_resolved_dataset_by_handle(&local_handle);

        tracing::debug!(local_target = ? local_target.handle, "Resolved push plan local target");

        match self
            .remote_alias_resolver
            .resolve_push_target(local_target, push_target.cloned())
            .await
        {
            Ok(remote_target) => Ok(PushItem {
                local_handle,
                remote_target,
                push_target: push_target.cloned(),
            }),
            Err(e) => Err(PushResponse {
                local_handle: Some(local_handle),
                target: push_target.cloned(),
                result: Err(e.into()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl PushRequestPlanner for PushRequestPlannerImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_handles, ?push_target))]
    async fn collect_plan(
        &self,
        dataset_handles: &[DatasetHandle],
        push_target: Option<&DatasetPushTarget>,
    ) -> (Vec<PushItem>, Vec<PushResponse>) {
        let mut plan = Vec::new();
        let mut errors = Vec::new();

        for hdl in dataset_handles {
            match self.collect_push_plan_item(hdl.clone(), push_target).await {
                Ok(item) => plan.push(item),
                Err(err) => errors.push(err),
            }
        }

        (plan, errors)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
