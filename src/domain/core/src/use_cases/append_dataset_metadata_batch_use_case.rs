// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AppendDatasetMetadataBatchUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset: &dyn odf::Dataset,
        new_blocks: VecDeque<odf::dataset::HashedMetadataBlock>,
        force_update_if_diverged: bool,
    ) -> Result<(), odf::dataset::AppendError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
