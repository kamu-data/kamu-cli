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

use crate::{ResourceHeaders, ResourceID, ResourceName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIdentityView {
    pub schema: String,
    pub canonical_kind_name: String,
    pub id: ResourceID,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub schema: String,
    pub account: ResourceViewAccount,
    pub headers: ResourceViewHeaders,
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
pub struct ResourceViewHeaders {
    pub id: ResourceID,
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

impl ResourceViewHeaders {
    pub fn simple(now: DateTime<Utc>, id: ResourceID, name: impl Into<ResourceName>) -> Self {
        Self {
            id,
            name: name.into(),
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            generation: 0,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    pub fn from_owned(id: ResourceID, headers: ResourceHeaders) -> Self {
        Self {
            id,
            name: headers.name,
            description: headers.description,
            labels: headers.labels,
            annotations: headers.annotations,
            generation: headers.generation,
            created_at: headers.created_at,
            updated_at: headers.updated_at,
            deleted_at: headers.deleted_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
