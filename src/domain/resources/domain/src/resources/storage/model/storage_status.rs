// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,
    pub provider_kind: StorageProviderKind,
    pub references: StorageReferenceStatus,
}

impl StorageStatus {
    pub fn new_pending(provider_kind: StorageProviderKind) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            provider_kind,
            references: StorageReferenceStatus::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageProviderKind {
    LocalFs,
    S3,
    Ipfs,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageReferenceStatus {
    pub total_references: usize,
    pub resolved_references: usize,
    pub unresolved_references: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
