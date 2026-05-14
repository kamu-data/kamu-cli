// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{DatasetEnvVarUpsertResult, DatasetEnvVarValue, DeleteDatasetEnvVarError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEnvVarMutationAdapter: Send + Sync {
    async fn upsert_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &DatasetEnvVarValue,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError>;

    async fn delete_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_env_var_key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
