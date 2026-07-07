// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources as domain;

use crate::facade::graphql::cynic_api::fragments;
use crate::facade::graphql::cynic_api::operations::supported_resource_types;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<supported_resource_types::ResourceTypeDescriptor> for domain::ResourceTypeDescriptor {
    fn from(value: supported_resource_types::ResourceTypeDescriptor) -> Self {
        Self {
            canonical_selector: value.canonical_selector,
            selector_aliases: value.selector_aliases,
            schema: value.schema,
            list_columns: value.list_columns.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<supported_resource_types::ResourceListColumnDescriptor>
    for domain::ResourceListColumnDescriptor
{
    fn from(value: supported_resource_types::ResourceListColumnDescriptor) -> Self {
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
        Ok(Self {
            schema: value.schema,
            headers: domain::ResourceViewHeaders {
                id: value.headers.id,
                account: domain::ResourceViewAccount {
                    id: value.headers.account.id.ok_or_else(|| {
                        InternalError::new(
                            "ResourceHeaders.account.id is required but missing".to_string(),
                        )
                    })?,
                    name: value
                        .headers
                        .account
                        .name
                        .map(|name| odf::AccountName::new_unchecked(&name.0)),
                },
                name: value.headers.name,
                description: value.headers.description,
                labels: value.headers.labels.0,
                annotations: value.headers.annotations.0,
                generation: value.headers.generation.into(),
                created_at: value.headers.created_at,
                updated_at: value.headers.updated_at,
                deleted_at: value.headers.deleted_at,
            },
            spec: value.spec,
            status: value.status.map(Into::into),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fragments::ResourceStatus> for domain::ResourceStatus {
    fn from(value: fragments::ResourceStatus) -> Self {
        Self {
            phase: value.phase.into(),
            observed_generation: value.observed_generation.map(Into::into),
            reconciled_at: value.reconciled_at,
            conditions: value.conditions.map(|conditions| conditions.0),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<fragments::ResourceIdentity> for domain::ResourceIdentityView {
    fn from(value: fragments::ResourceIdentity) -> Self {
        Self {
            schema: value.schema,
            canonical_selector: value.canonical_selector,
            id: value.id,
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
            observed_generation: s.observed_generation.map(Into::into),
            ready: s.ready,
        });

        let list_values = value
            .list_values
            .into_iter()
            .map(|v| {
                let key = v.key;
                let col_value = match (v.string_value, v.uint64_value, v.bool_value) {
                    (Some(s), None, None) => domain::ResourceListColumnValue::String(s),
                    (None, Some(n), None) => domain::ResourceListColumnValue::UInt64(n.into()),
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
            schema: value.schema,
            id: value.id,
            name: value.name,
            description: value.description,
            generation: value.generation.into(),
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
            schema: value.schema,
            canonical_selector: value.canonical_selector,
            total_count: value.total_count.into(),
            phase_counts: domain::ResourcePhaseCounts {
                pending: value.phase_counts.pending.into(),
                reconciling: value.phase_counts.reconciling.into(),
                ready: value.phase_counts.ready.into(),
                failed: value.phase_counts.failed.into(),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
