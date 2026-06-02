// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;

use crate::facade::graphql::cynic_api::fragments;
use crate::facade::graphql::cynic_api::operations::supported_kinds;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<supported_kinds::ResourceKindDescriptor> for domain::ResourceKindDescriptor {
    fn from(value: supported_kinds::ResourceKindDescriptor) -> Self {
        Self {
            name: value.name,
            short_names: value.short_names,
            kind: value.kind.value,
            api_version: value.api_version,
            list_columns: value.list_columns.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<supported_kinds::ResourceListColumnDescriptor> for domain::ResourceListColumnDescriptor {
    fn from(value: supported_kinds::ResourceListColumnDescriptor) -> Self {
        Self {
            key: value.key,
            header: value.header,
            data_type: value.data_type.into(),
            visibility: value.visibility.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<fragments::Resource> for domain::ResourceView {
    type Error = InternalError;

    fn try_from(value: fragments::Resource) -> Result<Self, Self::Error> {
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

impl From<fragments::ResourceIdentity> for domain::ResourceIdentityView {
    fn from(value: fragments::ResourceIdentity) -> Self {
        Self {
            kind: value.kind.value,
            api_version: value.api_version,
            canonical_kind_name: value.canonical_kind_name,
            uid: value.id,
            name: value.name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fragments::ResourceRenderManifestResult> for crate::RenderResourceManifestResult {
    fn from(value: fragments::ResourceRenderManifestResult) -> Self {
        Self {
            manifest: value.manifest,
            format: value.format.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<fragments::ResourceSummary> for domain::ResourceSummaryView {
    type Error = InternalError;

    fn try_from(value: fragments::ResourceSummary) -> Result<Self, Self::Error> {
        let status = value.status.map(|s| domain::ResourceStatusSummaryView {
            phase: s.phase.map(Into::into),
            observed_generation: s.observed_generation,
            ready: s.ready,
        });

        let list_values = value
            .list_values
            .into_iter()
            .map(|v| {
                let key = v.key;
                let col_value = match (v.string_value, v.uint64_value, v.bool_value) {
                    (Some(s), None, None) => domain::ResourceListColumnValue::String(s),
                    (None, Some(n), None) => domain::ResourceListColumnValue::UInt64(n),
                    (None, None, Some(b)) => domain::ResourceListColumnValue::Bool(b),
                    (None, None, None) => {
                        return Err(InternalError::new(format!(
                            "Missing list value payload for key '{key}'",
                        )));
                    }
                    _ => {
                        return Err(InternalError::new(format!(
                            "Ambiguous list value payload for key '{key}'",
                        )));
                    }
                };
                Ok(domain::ResourceListColumnValueView {
                    key,
                    value: col_value,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(domain::ResourceSummaryView {
            kind: value.kind.value,
            api_version: value.api_version,
            uid: value.id,
            name: value.name,
            description: value.description,
            generation: value.generation,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status,
            list_values,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<fragments::ResourcesSummary> for domain::ResourcesSummary {
    type Error = InternalError;

    fn try_from(value: fragments::ResourcesSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            resource_counts: value
                .resource_counts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<fragments::ResourceTypeCountSummary> for domain::ResourceTypeCountSummary {
    type Error = InternalError;

    fn try_from(value: fragments::ResourceTypeCountSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: value.kind,
            name: value.name,
            api_version: value.api_version,
            total_count: value.total_count,
            phase_counts: domain::ResourcePhaseCounts {
                pending: value.phase_counts.pending,
                reconciling: value.phase_counts.reconciling,
                ready: value.phase_counts.ready,
                degraded: value.phase_counts.degraded,
                failed: value.phase_counts.failed,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
