// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use engine::TransformRequestExt;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TransformElaborationService {}

    #[async_trait::async_trait]
    impl TransformElaborationService for TransformElaborationService {
        async fn elaborate_transform(
            &self,
            target: ResolvedDataset,
            plan: TransformPreliminaryPlan,
            transform_options: TransformOptions,
            maybe_listener: Option<Arc<dyn TransformListener>>,
        ) -> Result<TransformElaboration, TransformElaborateError>;
    }
}

impl MockTransformElaborationService {
    pub fn make_expect_elaborate_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_elaborate_transform()
            .withf(move |target, _, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(|_, plan, _, _| {
                Ok(TransformElaboration::Elaborated(TransformPlan {
                    request: TransformRequestExt {
                        operation_id: plan.preliminary_request.operation_id,
                        dataset_handle: plan.preliminary_request.dataset_handle,
                        block_ref: plan.preliminary_request.block_ref,
                        head: plan.preliminary_request.head,
                        transform: plan.preliminary_request.transform,
                        system_time: plan.preliminary_request.system_time,
                        schema: plan.preliminary_request.schema,
                        prev_offset: plan.preliminary_request.prev_offset,
                        vocab: plan.preliminary_request.vocab,
                        inputs: vec![],
                        prev_checkpoint: plan.preliminary_request.prev_checkpoint,
                    },
                    datasets_map: ResolvedDatasetsMap::default(),
                }))
            });
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
