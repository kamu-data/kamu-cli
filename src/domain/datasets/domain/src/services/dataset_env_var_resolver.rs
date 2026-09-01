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

/// Resolves dataset env vars from the variable sets and secret sets labelled as
/// targeting the dataset.
///
/// **Precedence is the same for both methods:** within each kind the oldest set
/// wins on key collision, and secrets override variables. A key carried by both
/// a variable set and a secret set therefore always resolves to the secret,
/// whether it is read through the whole map or looked up individually.
#[async_trait::async_trait]
pub trait DatasetEnvVarResolver: Send + Sync {
    /// The effective flat env-var map, merged per the precedence above. Used by
    /// update planning, not the legacy UI path.
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError>;

    /// A single entry by key, resolved per the same precedence. Kept separate
    /// from [`Self::resolve_effective_env_vars`] rather than implemented on top
    /// of it: this can early-exit on the first match, while building the whole
    /// map would load every entry of every targeting set to answer one lookup.
    async fn get_env_var_by_entry_key(
        &self,
        dataset_id: &odf::DatasetID,
        entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
