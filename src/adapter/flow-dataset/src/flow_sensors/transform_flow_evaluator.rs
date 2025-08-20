// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::TransformStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait TransformFlowEvaluator: Send + Sync {
    async fn evaluate_transform_status(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<TransformStatus, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
