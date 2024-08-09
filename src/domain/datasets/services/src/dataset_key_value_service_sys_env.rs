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
use kamu_core::ErrorIntoInternal;
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarValue,
    DatasetKeyValueService,
    FindDatasetEnvVarError,
};
use secrecy::Secret;

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

#[async_trait::async_trait]
impl DatasetKeyValueService for DatasetKeyValueServiceSysEnv {
    async fn find_dataset_env_var_value_by_key<'a>(
        &self,
        dataset_env_var_key: &str,
        _dataset_env_vars: &'a HashMap<String, DatasetEnvVar>,
    ) -> Result<DatasetEnvVarValue, FindDatasetEnvVarError> {
        match std::env::var(dataset_env_var_key) {
            Ok(value_string) => Ok(DatasetEnvVarValue::Secret(Secret::new(value_string))),
            Err(err) => Err(FindDatasetEnvVarError::Internal(err.int_err())),
        }
    }
}
