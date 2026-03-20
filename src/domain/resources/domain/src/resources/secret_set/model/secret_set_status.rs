// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{ResourceStatus, ResourceStatusLike};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretSetStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,

    pub stats: SecretSetStats,
}

impl SecretSetStatus {
    pub fn new_pending(stats: SecretSetStats) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            stats,
        }
    }
}

impl ResourceStatusLike for SecretSetStatus {
    fn resource_status(&self) -> &ResourceStatus {
        &self.resource_status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SecretSetStats {
    pub total_secrets: usize,
    pub valid_secrets: usize,
    pub invalid_secrets: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
