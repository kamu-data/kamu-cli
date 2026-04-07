// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use kamu_resources_facade::ResourceManifestFormat;

use crate::cli::ApplyManifestInputFormat;
use crate::resources::{
    DiscoverResourceManifestError,
    DiscoverResourceManifestsResult,
    DiscoveredResourceManifest,
    ResourceManifestDiscoveryService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceManifestDiscoveryService)]
pub struct ResourceManifestDiscoveryServiceImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceManifestDiscoveryService for ResourceManifestDiscoveryServiceImpl {
    fn discover(
        &self,
        inputs: Vec<PathBuf>,
        recursive: bool,
        format: Option<ApplyManifestInputFormat>,
    ) -> DiscoverResourceManifestsResult {
        let mut result = DiscoverResourceManifestsResult::default();

        for input in inputs {
            match self.discover_input(&input, recursive, format) {
                Ok(discovered) => result.manifests.extend(discovered),
                Err(err) => result.errors.push((input, err)),
            }
        }

        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceManifestDiscoveryServiceImpl {
    fn discover_input(
        &self,
        input: &Path,
        recursive: bool,
        format: Option<ApplyManifestInputFormat>,
    ) -> Result<Vec<DiscoveredResourceManifest>, DiscoverResourceManifestError> {
        let metadata =
            std::fs::metadata(input).map_err(DiscoverResourceManifestError::InspectPath)?;

        if metadata.is_dir() {
            return Self::collect_directory_manifests(input, recursive, format)?
                .into_iter()
                .map(|source| {
                    Ok(DiscoveredResourceManifest {
                        format: Self::resolve_manifest_format(&source, format)?,
                        source,
                    })
                })
                .collect();
        }

        if metadata.is_file() {
            return Ok(vec![DiscoveredResourceManifest {
                format: Self::resolve_manifest_format(input, format)?,
                source: input.to_path_buf(),
            }]);
        }

        Err(DiscoverResourceManifestError::UnsupportedPathType)
    }

    fn collect_directory_manifests(
        dir: &Path,
        recursive: bool,
        format: Option<ApplyManifestInputFormat>,
    ) -> Result<Vec<PathBuf>, DiscoverResourceManifestError> {
        let mut children = Vec::new();
        let entries =
            std::fs::read_dir(dir).map_err(DiscoverResourceManifestError::ReadDirectory)?;

        for entry in entries {
            let entry = entry.map_err(DiscoverResourceManifestError::ReadDirectory)?;
            children.push(entry.path());
        }

        children.sort();

        let mut manifests = Vec::new();
        for child in children {
            let metadata =
                std::fs::metadata(&child).map_err(DiscoverResourceManifestError::InspectPath)?;

            if metadata.is_dir() {
                if recursive {
                    manifests.extend(Self::collect_directory_manifests(
                        &child, recursive, format,
                    )?);
                }
            } else if metadata.is_file()
                && (format.is_some() || Self::is_supported_manifest_path(&child))
            {
                manifests.push(child);
            }
        }

        Ok(manifests)
    }

    fn resolve_manifest_format(
        path: &Path,
        format: Option<ApplyManifestInputFormat>,
    ) -> Result<ResourceManifestFormat, DiscoverResourceManifestError> {
        if let Some(format) = format {
            return Ok(match format {
                ApplyManifestInputFormat::Json => ResourceManifestFormat::Json,
                ApplyManifestInputFormat::Yaml => ResourceManifestFormat::Yaml,
            });
        }

        match Self::manifest_extension(path).as_deref() {
            Some("json") => Ok(ResourceManifestFormat::Json),
            Some("yaml" | "yml") => Ok(ResourceManifestFormat::Yaml),
            _ => Err(DiscoverResourceManifestError::UnsupportedExtension),
        }
    }

    fn manifest_extension(path: &Path) -> Option<String> {
        path.extension()
            .and_then(std::ffi::OsStr::to_str)
            .map(str::to_ascii_lowercase)
    }

    fn is_supported_manifest_path(path: &Path) -> bool {
        matches!(
            Self::manifest_extension(path).as_deref(),
            Some("json" | "yaml" | "yml")
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
