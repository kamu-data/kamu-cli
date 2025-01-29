// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::*;
use internal_error::ErrorIntoInternal;
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarValue,
    DatasetEnvVarsConfig,
    DatasetKeyValueService,
    FindDatasetEnvVarError,
};
use secrecy::{ExposeSecret, SecretString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyValueServiceImpl {
    dataset_env_var_encryption_key: SecretString,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetKeyValueService)]
impl DatasetKeyValueServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(dataset_env_var_config: Arc<DatasetEnvVarsConfig>) -> Self {
        Self {
            dataset_env_var_encryption_key: SecretString::from(
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

impl DatasetKeyValueService for DatasetKeyValueServiceImpl {
    fn find_dataset_env_var_value_by_key(
        &self,
        dataset_env_var_key: &str,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<DatasetEnvVarValue, FindDatasetEnvVarError> {
        if let Some(existing_dataset_env_var) = dataset_env_vars.get(dataset_env_var_key) {
            let exposed_value = existing_dataset_env_var
                .get_exposed_decrypted_value(self.dataset_env_var_encryption_key.expose_secret())
                .map_err(|err| FindDatasetEnvVarError::Internal(err.int_err()))?;
            return if existing_dataset_env_var.secret_nonce.is_some() {
                Ok(DatasetEnvVarValue::Secret(SecretString::from(
                    exposed_value,
                )))
            } else {
                return Ok(DatasetEnvVarValue::Regular(exposed_value));
            };
        }
        Err(FindDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_key.to_string(),
            },
        ))
    }
}
