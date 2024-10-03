// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::DatasetHandle;

use crate::{
    VerificationListener,
    VerificationMultiListener,
    VerificationRequest,
    VerificationResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait VerifyDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        request: VerificationRequest<DatasetHandle>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult;

    async fn execute_multi(
        &self,
        requests: Vec<VerificationRequest<DatasetHandle>>,
        maybe_multi_listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Vec<VerificationResult>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
