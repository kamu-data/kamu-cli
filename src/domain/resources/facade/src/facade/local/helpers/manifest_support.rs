// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{ResourceManifest, ResourceMetadataInput, ResourceView, ResourceWarning};

use crate::{
    ApplyManifestError,
    ParseResourceManifestError,
    ResolvedAccount,
    ResourceManifestFormat,
};

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

pub(crate) fn make_metadata_input(
    manifest: &ResourceManifest,
    target_account: &ResolvedAccount,
) -> Result<ResourceMetadataInput, ApplyManifestError> {
    ResourceMetadataInput::try_new(
        target_account.id.clone(),
        &manifest.metadata.name,
        manifest.metadata.description.clone(),
        manifest.metadata.labels.clone(),
        manifest.metadata.annotations.clone(),
    )
    .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn collect_manifest_metadata_warnings(
    manifest: &ResourceManifest,
) -> Vec<ResourceWarning> {
    let mut warnings = Vec::new();

    if manifest
        .metadata
        .description
        .as_ref()
        .is_none_or(|description| description.trim().is_empty())
    {
        warnings.push(ResourceWarning {
            code: WARNING_CODE_MISSING_DESCRIPTION.to_string(),
            path: Some("metadata.description".to_string()),
            message: "Resource has no description".to_string(),
        });
    }

    warnings
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resource_view_to_manifest(view: ResourceView) -> ResourceManifest {
    let ResourceView {
        kind,
        api_version,
        account,
        metadata,
        spec,
        ..
    } = view;

    ResourceManifest {
        api_version,
        kind,
        metadata: kamu_resources::ResourceManifestMetadata {
            uid: Some(metadata.uid),
            account: Some(kamu_resources::ResourceManifestAccount {
                id: Some(account.id),
                name: account.name.map(|name| name.to_string()),
            }),
            name: metadata.name,
            description: metadata.description,
            labels: metadata.labels.into_iter().collect(),
            annotations: metadata.annotations.into_iter().collect(),
        },
        spec,
    }
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
