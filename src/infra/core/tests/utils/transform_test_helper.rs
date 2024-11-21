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
use kamu::{
    TransformElaborationServiceImpl,
    TransformExecutionServiceImpl,
    TransformRequestPlannerImpl,
};
use kamu_core::{
    CompactionService,
    CreateDatasetResult,
    DatasetRegistry,
    EngineProvisioner,
    ResolvedDataset,
    TransformElaboration,
    TransformElaborationService,
    TransformExecutionService,
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
    transform_exec_svc: Arc<dyn TransformExecutionService>,
}

impl TransformTestHelper {
    pub fn build(
        dataset_registry: Arc<dyn DatasetRegistry>,
        system_time_source: Arc<dyn SystemTimeSource>,
        compaction_svc: Arc<dyn CompactionService>,
        engine_provisioner: Arc<dyn EngineProvisioner>,
    ) -> Self {
        Self {
            transform_request_planner: Arc::new(TransformRequestPlannerImpl::new(
                dataset_registry,
                system_time_source.clone(),
            )),
            transform_elab_svc: Arc::new(TransformElaborationServiceImpl::new(
                compaction_svc,
                system_time_source,
            )),
            transform_exec_svc: Arc::new(TransformExecutionServiceImpl::new(engine_provisioner)),
        }
    }

    pub fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            transform_request_planner: catalog.get_one().unwrap(),
            transform_elab_svc: catalog.get_one().unwrap(),
            transform_exec_svc: catalog.get_one().unwrap(),
        }
    }

    pub async fn transform_dataset(&self, derived: &CreateDatasetResult) -> TransformResult {
        let deriv_target = ResolvedDataset::from(derived);

        let plan = self
            .transform_request_planner
            .build_transform_preliminary_plan(deriv_target.clone())
            .await
            .unwrap();

        let plan = match self
            .transform_elab_svc
            .elaborate_transform(
                deriv_target.clone(),
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

        self.transform_exec_svc
            .execute_transform(deriv_target, plan, None)
            .await
            .1
            .unwrap()
    }

    pub async fn verify_transform(
        &self,
        derived: &CreateDatasetResult,
    ) -> Result<(), VerifyTransformError> {
        let deriv_target = ResolvedDataset::from(derived);

        let verify_plan = self
            .transform_request_planner
            .build_transform_verification_plan(deriv_target.clone(), (None, None))
            .await
            .map_err(VerifyTransformError::Plan)?;

        self.transform_exec_svc
            .execute_verify_transform(deriv_target, verify_plan, None)
            .await
            .map_err(VerifyTransformError::Execute)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
