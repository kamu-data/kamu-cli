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
use kamu_resources::{
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourcePhaseCounts,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceTypeCountSummary,
    ResourcesSummary,
};

use crate::facade::graphql::cynic_api::fragments;
use crate::facade::graphql::cynic_api::operations::supported_kinds;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn parse_enum<T>(value: &str, field_name: &str) -> Result<T, InternalError>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| {
        InternalError::new(format!(
            "Unsupported {field_name} '{value}' in remote resource list",
        ))
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<supported_kinds::ResourceKindDescriptor> for domain::ResourceKindDescriptor {
    type Error = InternalError;

    fn try_from(value: supported_kinds::ResourceKindDescriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name,
            short_names: value.short_names,
            kind: value.kind.value,
            api_version: value.api_version,
            list_columns: value
                .list_columns
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<supported_kinds::ResourceListColumnDescriptor>
    for domain::ResourceListColumnDescriptor
{
    type Error = InternalError;

    fn try_from(value: supported_kinds::ResourceListColumnDescriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            key: value.key,
            header: value.header,
            data_type: parse_enum(&value.data_type, "list column data type")?,
            visibility: parse_enum(&value.visibility, "list column visibility")?,
        })
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
                generation: u64::try_from(value.metadata.generation).int_err()?,
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

impl TryFrom<fragments::ResourceSummary> for ResourceSummaryView {
    type Error = InternalError;

    fn try_from(value: fragments::ResourceSummary) -> Result<Self, Self::Error> {
        let status = value
            .status
            .map(|s| {
                Ok(ResourceStatusSummaryView {
                    phase: s
                        .phase
                        .as_deref()
                        .map(|p| parse_enum(p, "resource phase"))
                        .transpose()?,
                    observed_generation: s
                        .observed_generation
                        .map(|g| u64::try_from(g).int_err())
                        .transpose()?,
                    ready: s.ready,
                })
            })
            .transpose()?;

        let list_values = value
            .list_values
            .into_iter()
            .map(|v| {
                let key = v.key;
                let col_value = match (v.string_value, v.uint64_value, v.bool_value) {
                    (Some(s), None, None) => ResourceListColumnValue::String(s),
                    (None, Some(n), None) => {
                        ResourceListColumnValue::UInt64(u64::try_from(n).int_err()?)
                    }
                    (None, None, Some(b)) => ResourceListColumnValue::Bool(b),
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
                Ok(ResourceListColumnValueView {
                    key,
                    value: col_value,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResourceSummaryView {
            kind: value.kind.value,
            api_version: value.api_version,
            uid: value.id,
            name: value.name,
            description: value.description,
            generation: u64::try_from(value.generation).int_err()?,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status,
            list_values,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<fragments::ResourcesSummary> for ResourcesSummary {
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

impl TryFrom<fragments::ResourceTypeCountSummary> for ResourceTypeCountSummary {
    type Error = InternalError;

    fn try_from(value: fragments::ResourceTypeCountSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: value.kind,
            name: value.name,
            api_version: value.api_version,
            total_count: u64::try_from(value.total_count).int_err()?,
            phase_counts: ResourcePhaseCounts {
                pending: u64::try_from(value.phase_counts.pending).int_err()?,
                reconciling: u64::try_from(value.phase_counts.reconciling).int_err()?,
                ready: u64::try_from(value.phase_counts.ready).int_err()?,
                degraded: u64::try_from(value.phase_counts.degraded).int_err()?,
                failed: u64::try_from(value.phase_counts.failed).int_err()?,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
