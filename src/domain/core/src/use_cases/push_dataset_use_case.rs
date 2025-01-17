// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;

use crate::{PushMultiOptions, PushResponse, SyncMultiListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushDatasetUseCase: Send + Sync {
    async fn execute_multi(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        options: PushMultiOptions,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PushResponse>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
