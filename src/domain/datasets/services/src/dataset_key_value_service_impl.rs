// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::ErrorIntoInternal;
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarsConfig,
    DatasetKeyValueService,
    GetDatasetEnvVarError,
};
use secrecy::{ExposeSecret, Secret};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyValueServiceImpl {
    dataset_env_var_encryption_key: Secret<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetKeyValueService)]
impl DatasetKeyValueServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(dataset_env_var_config: Arc<DatasetEnvVarsConfig>) -> Self {
        Self {
            dataset_env_var_encryption_key: Secret::new(
                dataset_env_var_config
                    .encryption_key
                    .as_ref()
                    .unwrap()
                    .clone(),
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyValueService for DatasetKeyValueServiceImpl {
    async fn get_dataset_env_var_value_by_key<'a>(
        &self,
        dataset_env_var_key: &str,
        dataset_env_vars: &'a [DatasetEnvVar],
    ) -> Result<Secret<String>, GetDatasetEnvVarError> {
        if let Some(existing_dataset_env_var) = dataset_env_vars
            .iter()
            .find(|dataset_env_var| dataset_env_var.key == dataset_env_var_key)
        {
            return Ok(Secret::new(
                existing_dataset_env_var
                    .get_exposed_value(self.dataset_env_var_encryption_key.expose_secret())
                    .map_err(|err| GetDatasetEnvVarError::Internal(err.int_err()))?,
            ));
        }
        Err(GetDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_key.to_string(),
            },
        ))
    }
}
