// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_core::ErrorIntoInternal;
use kamu_datasets::{DatasetEnvVar, DatasetKeyValueService, GetDatasetEnvVarError};
use secrecy::Secret;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyValueServiceStaticImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetKeyValueService)]
impl DatasetKeyValueServiceStaticImpl {
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyValueService for DatasetKeyValueServiceStaticImpl {
    async fn get_dataset_env_var_value_by_key<'a>(
        &self,
        dataset_env_var_key: &str,
        _dataset_env_vars: &'a [DatasetEnvVar],
    ) -> Result<Secret<String>, GetDatasetEnvVarError> {
        match std::env::var(dataset_env_var_key) {
            Ok(value_string) => Ok(Secret::new(value_string)),
            Err(err) => Err(GetDatasetEnvVarError::Internal(err.int_err())),
        }
    }
}
