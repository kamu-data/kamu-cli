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
    description_annotation_short_name_type_ref,
    description_annotation_type_ref,
};

use crate::{ApplyManifestError, ParseResourceManifestError, ResourceManifestFormat};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WARNING_CODE_MISSING_DESCRIPTION: &str = "missing_description";

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
    manifest: &ResourceManifest,
    target_account: &odf::AccountHandle,
) -> Result<ResourceHeadersInput, ApplyManifestError> {
    ResourceHeadersInputExt::try_new(
        Some(target_account.clone().into()),
        manifest.headers.name.as_str(),
        manifest.headers.labels.clone(),
        manifest.headers.annotations.clone(),
    )
    .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn collect_manifest_header_warnings(
    manifest: &ResourceManifest,
) -> Vec<ResourceWarning> {
    let mut warnings = Vec::new();

    let description = manifest
        .headers
        .annotations
        .iter()
        .find(|(key, _)| {
            *key == description_annotation_type_ref()
                || *key == description_annotation_short_name_type_ref()
        })
        .and_then(|(_, value)| value.as_str());

    if description.is_none_or(|description| description.trim().is_empty()) {
        warnings.push(ResourceWarning {
            code: WARNING_CODE_MISSING_DESCRIPTION.to_string(),
            path: Some("headers.annotations.description".to_string()),
            message: "Resource has no description".to_string(),
        });
    }

    warnings
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

    let account = Some(kamu_resources::ResourceAccountRef::Handle(headers.account));

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
