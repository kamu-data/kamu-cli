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

impl MockCompactionExecutor {
    /// Reports `HEAD` moved between the `heads` set by the time of execution,
    /// without moving any reference.
    pub fn make_expect_compaction_success(
        mut self,
        target_alias: odf::DatasetAlias,
        heads: ReportedHeads,
    ) -> Self {
        self.expect_execute()
            .withf(move |target, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(move |_, _, _| {
                let (old_head, new_head) = heads.get();
                Ok(CompactionResult::Success {
                    old_head,
                    new_head,
                    old_num_blocks: 2,
                    new_num_blocks: 1,
                    breaking: true,
                })
            });
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
