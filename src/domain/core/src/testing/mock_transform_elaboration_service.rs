// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_datasets::ResolvedDatasetsMap;

use crate::engine::TransformRequestExt;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformElaborationService {
    pub fn make_expect_elaborate_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_elaborate_transform()
            .withf(move |plan| plan.preliminary_request.dataset_handle.alias == target_alias)
            .times(1)
            .returning(|plan| {
                Ok(TransformElaboration::Elaborated(TransformPlan {
                    request: TransformRequestExt {
                        dataset_handle: plan.preliminary_request.dataset_handle,
                        block_ref: plan.preliminary_request.block_ref,
                        head: plan.preliminary_request.head,
                        transform: plan.preliminary_request.transform,
                        transform_block: plan.preliminary_request.transform_block,
                        system_time: Utc::now(),
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

    pub fn make_expect_elaborate_transform_up_to_date(
        mut self,
        target_alias: odf::DatasetAlias,
    ) -> Self {
        self.expect_elaborate_transform()
            .withf(move |plan| plan.preliminary_request.dataset_handle.alias == target_alias)
            .times(1)
            .returning(|_| Ok(TransformElaboration::UpToDate));
        self
    }

    /// Reports an input whose history was rewritten
    pub fn make_expect_elaborate_transform_diverged(self, target_alias: odf::DatasetAlias) -> Self {
        self.make_expect_elaborate_transform_error(target_alias, || {
            InvalidInputIntervalError {
                input_dataset_id: odf::DatasetID::new_seeded_ed25519(b"input"),
                head: odf::Multihash::from_digest_sha3_256(b"head"),
                tail: odf::Multihash::from_digest_sha3_256(b"tail"),
            }
            .into()
        })
    }

    pub fn make_expect_elaborate_transform_error(
        mut self,
        target_alias: odf::DatasetAlias,
        make_error: impl Fn() -> TransformElaborateError + Send + 'static,
    ) -> Self {
        self.expect_elaborate_transform()
            .withf(move |plan| plan.preliminary_request.dataset_handle.alias == target_alias)
            .times(1)
            .returning(move |_| Err(make_error()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
