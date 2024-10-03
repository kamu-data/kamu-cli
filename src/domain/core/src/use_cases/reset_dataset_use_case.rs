// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetHandle, Multihash};

use crate::ResetError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResetDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        maybe_new_head: Option<&Multihash>,
        maybe_old_head: Option<&Multihash>,
    ) -> Result<Multihash, ResetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
