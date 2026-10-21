// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_resources::Resource;

use super::{CLIError, common};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Renders a resource with all its server-maintained headers and its status as
/// a YAML manifest
pub(crate) fn render_resource_yaml(resource: &Resource) -> Result<String, CLIError> {
    serde_yaml::to_string(&RenderedResource {
        schema: resource.schema.as_str(),
        headers: RenderedResourceHeaders::new(resource),
        spec: common::json_to_yaml_value(&resource.spec),
        status: common::json_to_yaml_value(&kamu_resources::resource_status_to_json(
            &resource.status,
        )),
    })
    .map_err(CLIError::critical)
}

/// Renders a resource with all its server-maintained headers and its status as
/// a pretty-printed JSON manifest
pub(crate) fn render_resource_json(resource: &Resource) -> Result<String, CLIError> {
    serde_json::to_string_pretty(&RenderedResource {
        schema: resource.schema.as_str(),
        headers: RenderedResourceHeaders::new(resource),
        spec: &resource.spec,
        status: kamu_resources::resource_status_to_json(&resource.status),
    })
    .map_err(CLIError::critical)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize)]
struct RenderedResource<'a, TSpec, TStatus> {
    #[serde(rename = "$schema")]
    schema: &'a str,
    headers: RenderedResourceHeaders<'a>,
    spec: TSpec,
    status: TStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RenderedResourceHeaders<'a> {
    id: &'a kamu_resources::ResourceID,
    #[serde_as(as = "odf::metadata::serde::yaml::auth::ResourceOwnerHandle")]
    account: &'a odf::ResourceOwnerHandle,
    name: &'a str,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceLabels")]
    labels: &'a kamu_resources::ResourceLabels,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceAnnotations")]
    annotations: &'a kamu_resources::ResourceAnnotations,
    generation: u64,
    status_generation: u64,
    created_at: &'a DateTime<Utc>,
    updated_at: &'a DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_requested_at: &'a Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_at: &'a Option<DateTime<Utc>>,
}

impl<'a> RenderedResourceHeaders<'a> {
    fn new(resource: &'a Resource) -> Self {
        Self {
            id: &resource.headers.id,
            account: &resource.headers.account,
            name: &resource.headers.name,
            labels: &resource.headers.labels,
            annotations: &resource.headers.annotations,
            generation: resource.headers.generation,
            status_generation: resource.headers.status_generation,
            created_at: &resource.headers.created_at,
            updated_at: &resource.headers.updated_at,
            deletion_requested_at: &resource.headers.deletion_requested_at,
            deleted_at: &resource.headers.deleted_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
