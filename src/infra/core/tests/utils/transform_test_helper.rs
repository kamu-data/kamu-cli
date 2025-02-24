// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::Catalog;
use kamu::{TransformElaborationServiceImpl, TransformExecutorImpl, TransformRequestPlannerImpl};
use kamu_core::{
    CompactionExecutor,
    CompactionPlanner,
    DatasetRegistry,
    EngineProvisioner,
    ResolvedDataset,
    TransformElaboration,
    TransformElaborationService,
    TransformExecutor,
    TransformOptions,
    TransformRequestPlanner,
    TransformResult,
    VerifyTransformError,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformTestHelper {
    transform_request_planner: Arc<dyn TransformRequestPlanner>,
    transform_elab_svc: Arc<dyn TransformElaborationService>,
    transform_executor: Arc<dyn TransformExecutor>,
}

impl TransformTestHelper {
    pub fn build(
        dataset_registry: Arc<dyn DatasetRegistry>,
        system_time_source: Arc<dyn SystemTimeSource>,
        compaction_planner: Arc<dyn CompactionPlanner>,
        compaction_executor: Arc<dyn CompactionExecutor>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            transform_request_planner: Arc::new(TransformRequestPlannerImpl::new(
                dataset_registry,
                system_time_source.clone(),
            )),
            transform_elab_svc: Arc::new(TransformElaborationServiceImpl::new(
                compaction_planner,
                compaction_executor,
                system_time_source,
            )),
            transform_executor: Arc::new(TransformExecutorImpl::new(engine_provisioner)),
        }
    }

    pub fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            transform_request_planner: catalog.get_one().unwrap(),
            transform_elab_svc: catalog.get_one().unwrap(),
            transform_executor: catalog.get_one().unwrap(),
        }
    }

    pub async fn transform_dataset(&self, derived_target: ResolvedDataset) -> TransformResult {
        let plan = self
            .transform_request_planner
            .build_transform_preliminary_plan(derived_target.clone())
            .await
            .unwrap();

        let plan = match self
            .transform_elab_svc
            .elaborate_transform(
                derived_target.clone(),
                plan,
                TransformOptions::default(),
                None,
            )
            .await
            .unwrap()
        {
            TransformElaboration::Elaborated(plan) => plan,
            TransformElaboration::UpToDate => return TransformResult::UpToDate,
        };

        self.transform_executor
            .execute_transform(derived_target, plan, None)
            .await
            .1
            .unwrap()
    }

    pub async fn verify_transform(
        &self,
        derived_target: ResolvedDataset,
    ) -> Result<(), VerifyTransformError> {
        let verify_plan = self
            .transform_request_planner
            .build_transform_verification_plan(derived_target.clone(), (None, None))
            .await
            .map_err(VerifyTransformError::Plan)?;

        self.transform_executor
            .execute_verify_transform(derived_target, verify_plan, None)
            .await
            .map_err(VerifyTransformError::Execute)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
