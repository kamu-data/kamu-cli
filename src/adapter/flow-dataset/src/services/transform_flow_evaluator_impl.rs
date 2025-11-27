// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{TransformRequestPlanner, TransformStatus, TransformStatusError};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};

use crate::TransformFlowEvaluator;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn TransformFlowEvaluator)]
pub struct TransformFlowEvaluatorImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    transform_request_planner: Arc<dyn TransformRequestPlanner>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransformFlowEvaluator for TransformFlowEvaluatorImpl {
    async fn evaluate_transform_status(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<TransformStatus, InternalError> {
        let target = self
            .dataset_registry
            .get_dataset_by_id(dataset_id)
            .await
            .int_err()?;

        self.transform_request_planner
            .evaluate_transform_status(target)
            .await
            .map_err(|e| match e {
                TransformStatusError::TransformNotDefined(e) => e.int_err(),
                TransformStatusError::Internal(e) => e,
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
