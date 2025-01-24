// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ViewDatasetUseCaseError, ViewMultiResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait EditDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, EditDatasetUseCaseError>;

    async fn execute_multi(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
    ) -> Result<EditMultiResult, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EditMultiResult = ViewMultiResult;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EditDatasetUseCaseError = ViewDatasetUseCaseError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
