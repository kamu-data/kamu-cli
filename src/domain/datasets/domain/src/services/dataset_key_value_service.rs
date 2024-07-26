// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use secrecy::Secret;

use crate::{DatasetEnvVar, GetDatasetEnvVarError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetKeyValueService: Sync + Send {
    async fn get_dataset_env_var_value_by_key<'a>(
        &self,
        dataset_env_var_key: &str,
        dataset_env_vars: &'a [DatasetEnvVar],
    ) -> Result<Secret<String>, GetDatasetEnvVarError>;
}
