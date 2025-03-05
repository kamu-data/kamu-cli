// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_datasets::DatasetEnvVar;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ViewDatasetEnvVar {
    env_var: DatasetEnvVar,
}

#[Object]
impl ViewDatasetEnvVar {
    #[graphql(skip)]
    pub fn new(env_var: DatasetEnvVar) -> Self {
        Self { env_var }
    }

    /// Unique identifier of the dataset environment variable
    async fn id(&self) -> DatasetEnvVarID {
        self.env_var.id.into()
    }

    /// Key of the dataset environment variable
    async fn key(&self) -> String {
        self.env_var.key.clone()
    }

    /// Non secret value of dataset environment variable
    #[allow(clippy::unused_async)]
    async fn value(&self) -> Option<String> {
        self.env_var.get_non_secret_value()
    }

    /// Date of the dataset environment variable creation
    async fn created_at(&self) -> DateTime<Utc> {
        self.env_var.created_at
    }

    async fn is_secret(&self) -> bool {
        self.env_var.secret_nonce.is_some()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
