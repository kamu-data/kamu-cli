// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::*;
use kamu_datasets::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TransformRequestPlanner {}
    #[async_trait::async_trait]
    impl TransformRequestPlanner for TransformRequestPlanner {
        async fn build_transform_preliminary_plan(
            &self,
            target: ResolvedDataset,
        ) -> Result<TransformPreliminaryPlan, TransformPlanError>;

        async fn build_transform_verification_plan(
            &self,
            target: ResolvedDataset,
            block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
        ) -> Result<VerifyTransformOperation, VerifyTransformPlanError>;

        async fn evaluate_transform_status(
            &self,
            target: ResolvedDataset,
        ) -> Result<TransformStatus, TransformStatusError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
