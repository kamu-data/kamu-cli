// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformExecutor {
    pub fn make_expect_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_execute_transform()
            .withf(move |target, _, _| target.get_alias() == &target_alias)
            .times(1)
            .returning(|target, _, _| (target, Ok(TransformResult::UpToDate)));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
