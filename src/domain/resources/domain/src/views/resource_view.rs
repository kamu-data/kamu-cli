// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ResourceAnnotations,
    ResourceHeaders,
    ResourceID,
    ResourceLabels,
    ResourceName,
    ResourceSelectorName,
    ResourceStatus,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceIdentityView {
    pub schema: TypeUri,
    pub canonical_selector: ResourceSelectorName,
    pub id: ResourceID,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceView {
    pub schema: TypeUri,
    pub headers: ResourceViewHeaders,
    pub spec: serde_json::Value,
    #[serde_as(as = "Option<odf::metadata::serde::yaml::resource::ResourceStatus>")]
    pub status: Option<ResourceStatus>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceViewAccount {
    pub id: odf::AccountID,
    pub name: Option<odf::AccountName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceViewHeaders {
    pub id: ResourceID,
    pub account: ResourceViewAccount,
    pub name: ResourceName,
    pub description: Option<String>,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceLabels")]
    pub labels: ResourceLabels,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceAnnotations")]
    pub annotations: ResourceAnnotations,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceViewHeaders {
    pub fn simple(
        now: DateTime<Utc>,
        id: ResourceID,
        account: ResourceViewAccount,
        name: &str,
    ) -> Self {
        Self {
            id,
            account,
            name: ResourceName::new_unchecked(name),
            description: None,
            labels: ResourceLabels {
                entries: std::collections::BTreeMap::new(),
            },
            annotations: ResourceAnnotations {
                entries: std::collections::BTreeMap::new(),
            },
            generation: 0,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    pub fn from_owned(id: ResourceID, headers: ResourceHeaders) -> Self {
        let account = ResourceViewAccount {
            id: headers.account.clone(),
            name: None,
        };

        Self {
            id,
            account,
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

    pub fn with_account(mut self, account: ResourceViewAccount) -> Self {
        self.account = account;
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
