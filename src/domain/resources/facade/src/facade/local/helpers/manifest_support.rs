// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    Resource,
    ResourceHeadersInput,
    ResourceHeadersInputExt,
    ResourceManifest,
    ResourceWarning,
    description_annotation_type_ref,
    resource_label_not_indexed_warning,
    resource_missing_description_warning,
};

use crate::{ApplyManifestError, ParseResourceManifestError, ResourceManifestFormat};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn parse_manifest(
    format: ResourceManifestFormat,
    manifest: &str,
) -> Result<ResourceManifest, ParseResourceManifestError> {
    match format {
        ResourceManifestFormat::Json => {
            serde_json::from_str(manifest).map_err(|e| ParseResourceManifestError {
                message: format!("input is not valid JSON: {e}"),
            })
        }
        ResourceManifestFormat::Yaml => {
            serde_yaml::from_str(manifest).map_err(|e| ParseResourceManifestError {
                message: format!("input is not valid YAML: {e}"),
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_headers_input(
    name: &str,
    target_account: &odf::AccountHandle,
    labels: Vec<(kamu_resources::TypeRef, serde_json::Value)>,
    annotations: Vec<(kamu_resources::TypeRef, serde_json::Value)>,
) -> Result<ResourceHeadersInput, ApplyManifestError> {
    ResourceHeadersInputExt::try_new(
        Some(odf::metadata::auth::AccountRef {
            id: Some(target_account.id),
            did: Some(target_account.did.clone()),
            name: Some(target_account.name.clone()),
        }),
        name,
        labels,
        annotations,
    )
    .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn collect_manifest_header_warnings(
    annotations: &[(kamu_resources::TypeRef, serde_json::Value)],
) -> Vec<ResourceWarning> {
    let mut warnings = Vec::new();

    let description = annotations
        .iter()
        .find(|(key, _)| *key == description_annotation_type_ref())
        .and_then(|(_, value)| value.as_str());

    if description.is_none_or(|description| description.trim().is_empty()) {
        warnings.push(resource_missing_description_warning());
    }

    warnings
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn collect_non_indexable_label_warnings(
    labels: &[(kamu_resources::TypeRef, serde_json::Value)],
) -> Vec<ResourceWarning> {
    labels
        .iter()
        .filter(|(_, value)| !value.is_string())
        .map(|(key, _)| resource_label_not_indexed_warning(key))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_to_manifest(view: Resource) -> Result<ResourceManifest, InternalError> {
    let Resource {
        schema,
        headers,
        spec,
        ..
    } = view;

    // `schema` originates from a stored, already-canonical resource, so parsing it
    // back into a `ResourceSchema` is infallible in practice; treat a failure as a
    // store-integrity bug.
    let schema = kamu_resources::ResourceSchemaId::parse(schema.as_str()).int_err()?;

    let account = Some(kamu_resources::ResourceAccountRef {
        id: Some(headers.account.id),
        did: Some(headers.account.did),
        name: Some(headers.account.name),
    });

    Ok(ResourceManifest {
        schema,
        headers: kamu_resources::ResourceManifestHeaders {
            id: None,
            account,
            name: headers.name.to_string(),
            labels: headers.labels.entries.into_iter().collect(),
            annotations: headers.annotations.entries.into_iter().collect(),
        },
        spec,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn serialize_manifest(
    manifest: &ResourceManifest,
    format: ResourceManifestFormat,
) -> Result<String, InternalError> {
    match format {
        ResourceManifestFormat::Json => serde_json::to_string_pretty(manifest).int_err(),
        ResourceManifestFormat::Yaml => serde_yaml::to_string(manifest).int_err(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
