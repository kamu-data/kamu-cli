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
use kamu::{TransformExecutorImpl, TransformPlannerImpl};
use kamu_core::{
    EngineProvisioner,
    TransformElaborateError,
    TransformElaboration,
    TransformExecuteError,
    TransformExecutionPlan,
    TransformExecutor,
    TransformPlanner,
    TransformResult,
    VerifyTransformError,
};
use kamu_datasets::{DatasetRegistry, ResolvedDataset};
use random_strings::get_random_name;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformTestHelper {
    transform_planner: Arc<dyn TransformPlanner>,
    transform_executor: Arc<dyn TransformExecutor>,
}

impl TransformTestHelper {
    pub fn build(
        dataset_registry: Arc<dyn DatasetRegistry>,
        system_time_source: Arc<dyn SystemTimeSource>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            transform_planner: Arc::new(TransformPlannerImpl::new(
                dataset_registry,
                system_time_source,
            )),
            transform_executor: Arc::new(TransformExecutorImpl::new(engine_provisioner)),
        }
    }

    pub fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            transform_planner: catalog.get_one().unwrap(),
            transform_executor: catalog.get_one().unwrap(),
        }
    }

    pub async fn elaborate_transform(
        &self,
        derived_target: ResolvedDataset,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        self.transform_planner
            .elaborate_transform(derived_target)
            .await
    }

    /// Runs the engine and appends the blocks, without moving `HEAD`
    pub async fn execute_transform(
        &self,
        derived_target: ResolvedDataset,
        plan: TransformExecutionPlan,
        operation_id: &str,
    ) -> TransformResult {
        self.try_execute_transform(derived_target, plan, operation_id)
            .await
            .unwrap()
    }

    pub async fn try_execute_transform(
        &self,
        derived_target: ResolvedDataset,
        plan: TransformExecutionPlan,
        operation_id: &str,
    ) -> Result<TransformResult, TransformExecuteError> {
        self.transform_executor
            .execute_transform(derived_target, plan, operation_id, None)
            .await
            .1
    }

    pub async fn transform_dataset(&self, derived_target: ResolvedDataset) -> TransformResult {
        let plan = match self
            .elaborate_transform(derived_target.clone())
            .await
            .unwrap()
        {
            TransformElaboration::Elaborated(plan) => plan,
            TransformElaboration::UpToDate => return TransformResult::UpToDate,
        };

        let transform_result = self
            .execute_transform(derived_target.clone(), plan, &get_random_name(None, 10))
            .await;

        if let TransformResult::Updated {
            old_head, new_head, ..
        } = &transform_result
        {
            derived_target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .unwrap();
        }

        transform_result
    }

    pub async fn verify_transform(
        &self,
        derived_target: ResolvedDataset,
    ) -> Result<(), VerifyTransformError> {
        let verify_plan = self
            .transform_planner
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
