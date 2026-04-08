// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RenderManifestQueryDataFragment {
    pub resources: RenderManifestResourcesFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RenderManifestResourcesFragment {
    pub render_manifest: ResourceRenderManifestResultFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminRenderManifestQueryDataFragment {
    pub admin: AdminRenderManifestFieldFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdminRenderManifestFieldFragment {
    pub resources: RenderManifestResourcesFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceRenderManifestResultFragment {
    pub manifest: String,
    pub format: ResourceManifestFormatFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub(crate) enum ResourceManifestFormatFragment {
    Json,
    Yaml,
}

impl From<ResourceManifestFormatFragment> for crate::ResourceManifestFormat {
    fn from(value: ResourceManifestFormatFragment) -> Self {
        match value {
            ResourceManifestFormatFragment::Json => Self::Json,
            ResourceManifestFormatFragment::Yaml => Self::Yaml,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
