// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use internal_error::InternalError;

use crate::{DatasetEnvVar, GetDatasetEnvVarError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the effective flat env-var map for a dataset by merging all
/// ordered variable-set and secret-set bindings. Secrets override variables on
/// key collision. Used by update planning, not the legacy UI path.
#[async_trait::async_trait]
pub trait DatasetEnvVarResolver: Send + Sync {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError>;

    async fn get_env_var_by_entry_key(
        &self,
        dataset_id: &odf::DatasetID,
        entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
