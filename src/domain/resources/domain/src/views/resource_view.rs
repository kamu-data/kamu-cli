// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ResourceMetadata, ResourceName, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIdentityView {
    pub kind: String,
    pub api_version: String,
    pub canonical_kind_name: String,
    pub uid: ResourceUID,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub kind: String,
    pub api_version: String,
    pub account: ResourceViewAccount,
    pub metadata: ResourceViewMetadata,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceViewAccount {
    pub id: odf::AccountID,
    pub name: Option<odf::AccountName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceViewMetadata {
    pub uid: ResourceUID,
    pub name: ResourceName,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceViewMetadata {
    pub fn from_owned(uid: ResourceUID, metadata: ResourceMetadata) -> Self {
        Self {
            uid,
            name: metadata.name,
            description: metadata.description,
            labels: metadata.labels,
            annotations: metadata.annotations,
            generation: metadata.generation,
            created_at: metadata.created_at,
            updated_at: metadata.updated_at,
            deleted_at: metadata.deleted_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
