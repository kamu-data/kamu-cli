// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarListing,
    DatasetEnvVarService,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
};
use opendatafabric::DatasetID;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarServiceNull {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarService)]
impl DatasetEnvVarServiceNull {
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarService for DatasetEnvVarServiceNull {
    async fn upsert_dataset_env_var(
        &self,
        _dataset_env_var_key: &str,
        _dataset_env_var_value: &DatasetEnvVarValue,
        _dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        unreachable!()
    }

    async fn get_exposed_value(
        &self,
        _dataset_env_var: &DatasetEnvVar,
    ) -> Result<String, InternalError> {
        unreachable!()
    }

    async fn get_dataset_env_var_by_id(
        &self,
        _dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        unreachable!()
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        _dataset_id: &DatasetID,
        _pagination: Option<PaginationOpts>,
    ) -> Result<DatasetEnvVarListing, GetDatasetEnvVarError> {
        Ok(DatasetEnvVarListing {
            list: vec![],
            total_count: 0,
        })
    }

    async fn delete_dataset_env_var(
        &self,
        _dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        unreachable!()
    }
}
