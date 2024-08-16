// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetHandle, MetadataEvent};

use crate::{CommitError, CommitOpts, CommitResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CommitDatasetEventUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        event: MetadataEvent,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
