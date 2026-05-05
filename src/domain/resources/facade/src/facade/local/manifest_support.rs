// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::{ResourceManifest, ResourceMetadataInput, ResourceView, ResourceWarning};
use internal_error::ResultIntoInternal;
use kamu_resources as domain;

use crate::{
    ApplyManifestError,
    ParseResourceManifestError,
    RenderResourceManifestError,
    ResolvedAccount,
    ResourceManifestFormat,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WARNING_CODE_MISSING_DESCRIPTION: &str = "missing_description";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn parse_manifest(
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

pub(super) fn make_metadata_input(
    manifest: &ResourceManifest,
    target_account: &ResolvedAccount,
) -> Result<ResourceMetadataInput, ApplyManifestError> {
    ResourceMetadataInput::try_new(
        target_account.id.clone(),
        manifest.metadata.name.clone(),
        manifest.metadata.description.clone(),
        manifest.metadata.labels.clone(),
        manifest.metadata.annotations.clone(),
    )
    .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn collect_manifest_metadata_warnings(
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
            code: WARNING_CODE_MISSING_DESCRIPTION,
            path: Some("metadata.description".to_string()),
            message: "Resource has no description".to_string(),
        });
    }

    warnings
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn resource_view_to_manifest(view: ResourceView) -> ResourceManifest {
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

pub(super) fn serialize_manifest(
    manifest: &ResourceManifest,
    format: ResourceManifestFormat,
) -> Result<String, RenderResourceManifestError> {
    match format {
        ResourceManifestFormat::Json => serde_json::to_string_pretty(manifest)
            .int_err()
            .map_err(Into::into),
        ResourceManifestFormat::Yaml => serde_yaml::to_string(manifest)
            .int_err()
            .map_err(Into::into),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
