// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use dill::*;
use internal_error::*;
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarValue,
    DatasetKeyValueService,
    FindDatasetEnvVarError,
};
use secrecy::SecretString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyValueServiceSysEnv {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetKeyValueService)]
impl DatasetKeyValueServiceSysEnv {
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetKeyValueService for DatasetKeyValueServiceSysEnv {
    fn find_dataset_env_var_value_by_key(
        &self,
        dataset_env_var_key: &str,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
    ) -> Result<DatasetEnvVarValue, FindDatasetEnvVarError> {
        // Search explicitly provided vars first
        if let Some(var) = dataset_env_vars.get(dataset_env_var_key) {
            return Ok(DatasetEnvVarValue::Regular(
                var.get_non_secret_value()
                    .expect("Unexpected encrypted value"),
            ));
        }

        // Fall back to system environment
        match std::env::var(dataset_env_var_key) {
            Ok(value_string) => Ok(DatasetEnvVarValue::Secret(SecretString::from(value_string))),
            Err(std::env::VarError::NotPresent) => Err(FindDatasetEnvVarError::NotFound(
                DatasetEnvVarNotFoundError {
                    dataset_env_var_key: dataset_env_var_key.to_owned(),
                },
            )),
            Err(err) => Err(FindDatasetEnvVarError::Internal(err.int_err())),
        }
    }
}
