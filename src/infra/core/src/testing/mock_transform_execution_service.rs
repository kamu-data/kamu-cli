// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::*;
use kamu_datasets::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TransformExecutionService {}

    #[async_trait::async_trait]
    impl TransformExecutor for TransformExecutionService {
        async fn execute_transform(
            &self,
            target: ResolvedDataset,
            plan: TransformPlan,
            maybe_listener: Option<Arc<dyn TransformListener>>,
        ) -> (
            ResolvedDataset,
            Result<TransformResult, TransformExecuteError>,
        );

        async fn execute_verify_transform(
            &self,
            target: ResolvedDataset,
            verification_operation: VerifyTransformOperation,
            maybe_listener: Option<Arc<dyn VerificationListener>>,
        ) -> Result<(), VerifyTransformExecuteError>;
    }
}

impl MockTransformExecutionService {
    pub fn make_expect_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_execute_transform()
            .withf(move |target, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(|target, _, _| (target, Ok(TransformResult::UpToDate)));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
