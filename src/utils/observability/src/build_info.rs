// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Built information intended to be filled out by the `vergen` crate.
///
/// The `vergen` crate has to act on the level of the binary crate being built,
/// so we cannot add code that collects these values in here.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub app_version: &'static str,
    pub build_timestamp: Option<&'static str>,
    pub git_describe: Option<&'static str>,
    pub git_sha: Option<&'static str>,
    pub git_commit_date: Option<&'static str>,
    pub git_branch: Option<&'static str>,
    pub rustc_semver: Option<&'static str>,
    pub rustc_channel: Option<&'static str>,
    pub rustc_host_triple: Option<&'static str>,
    pub rustc_commit_sha: Option<&'static str>,
    pub cargo_target_triple: Option<&'static str>,
    pub cargo_features: Option<&'static str>,
    pub cargo_opt_level: Option<&'static str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Axum handler for serving the build info. Depends on [`dill::Catalog`] and it
/// having [`BuildInfo`] available.
#[expect(clippy::unused_async)]
pub async fn build_info_handler(
    axum::Extension(catalog): axum::Extension<dill::Catalog>,
) -> axum::Json<BuildInfo> {
    let build_info = catalog.get_one::<BuildInfo>().unwrap();
    axum::Json(build_info.as_ref().clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
