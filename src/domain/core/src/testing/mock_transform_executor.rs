// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ReportedHeads;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformExecutor {
    /// Increment that the mocked updates report
    pub const REPORTED_INCREMENT: odf::dataset::MetadataChainIncrementInterval =
        odf::dataset::MetadataChainIncrementInterval {
            num_blocks: 1,
            num_records: 5,
            updated_watermark: None,
        };

    pub fn make_expect_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_execute_transform()
            .withf(move |target, _, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(|target, _, _, _| (target, Ok(TransformResult::UpToDate)));
        self
    }

    /// Reports `HEAD` moved between the `heads` set by the time of execution,
    /// without moving any reference.
    pub fn make_expect_transform_updated(
        mut self,
        target_alias: odf::DatasetAlias,
        heads: ReportedHeads,
    ) -> Self {
        self.expect_execute_transform()
            .withf(move |target, _, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(move |target, _, _, _| {
                let (old_head, new_head) = heads.get();
                (
                    target,
                    Ok(TransformResult::Updated {
                        old_head,
                        new_head,
                        new_offset_interval: Some(odf::metadata::OffsetInterval {
                            start: 0,
                            end: Self::REPORTED_INCREMENT.num_records - 1,
                        }),
                        increment: Self::REPORTED_INCREMENT,
                    }),
                )
            });
        self
    }

    pub fn make_expect_transform_error(
        mut self,
        target_alias: odf::DatasetAlias,
        make_error: impl Fn() -> TransformExecuteError + Send + 'static,
    ) -> Self {
        self.expect_execute_transform()
            .withf(move |target, _, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(move |target, _, _, _| (target, Err(make_error())));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
