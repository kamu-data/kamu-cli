// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use kamu_resources_facade::ResourceManifestFormat;

use crate::cli::ApplyManifestInputFormat;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceManifestDiscoveryService: Send + Sync {
    fn discover(
        &self,
        inputs: Vec<PathBuf>,
        recursive: bool,
        format: Option<ApplyManifestInputFormat>,
    ) -> DiscoverResourceManifestsResult;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct DiscoverResourceManifestsResult {
    pub manifests: Vec<DiscoveredResourceManifest>,
    pub errors: Vec<(PathBuf, DiscoverResourceManifestError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DiscoveredResourceManifest {
    pub source: PathBuf,
    pub format: ResourceManifestFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum DiscoverResourceManifestError {
    #[error("Failed to inspect input path")]
    InspectPath(#[source] std::io::Error),

    #[error("Input path is neither a regular file nor a directory")]
    UnsupportedPathType,

    #[error("Unsupported manifest extension; expected .yaml, .yml, or .json, or pass --format")]
    UnsupportedExtension,

    #[error("Failed to read directory")]
    ReadDirectory(#[source] std::io::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
