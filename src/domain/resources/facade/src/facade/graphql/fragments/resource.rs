// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;
use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceFragment {
    pub api_version: String,
    pub kind: ResourceKindFragment,
    pub metadata: ResourceMetadataFragment,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

impl TryFrom<ResourceFragment> for domain::ResourceView {
    type Error = InternalError;

    fn try_from(value: ResourceFragment) -> Result<Self, Self::Error> {
        let labels: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.labels).int_err()?;
        let annotations: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.annotations).int_err()?;

        Ok(Self {
            kind: value.kind.value,
            api_version: value.api_version,
            account: domain::ResourceViewAccount {
                id: value.metadata.account_id,
                name: None,
            },
            metadata: domain::ResourceViewMetadata {
                uid: value.metadata.id,
                name: value.metadata.name,
                description: value.metadata.description,
                labels,
                annotations,
                generation: value.metadata.generation,
                created_at: value.metadata.created_at,
                updated_at: value.metadata.updated_at,
                deleted_at: value.metadata.deleted_at,
            },
            last_reconciled_at: value.metadata.last_reconciled_at,
            spec: value.spec,
            status: value.status,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ResourceKindFragment {
    pub value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceMetadataFragment {
    pub id: domain::ResourceUID,
    pub account_id: odf::AccountID,
    pub name: String,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
