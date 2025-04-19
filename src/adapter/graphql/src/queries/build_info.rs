// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(async_graphql::SimpleObject)]
pub struct BuildInfo {
    app_version: &'static str,
    build_timestamp: Option<&'static str>,
    git_describe: Option<&'static str>,
    git_sha: Option<&'static str>,
    git_commit_date: Option<&'static str>,
    git_branch: Option<&'static str>,
    rustc_semver: Option<&'static str>,
    rustc_channel: Option<&'static str>,
    rustc_host_triple: Option<&'static str>,
    rustc_commit_sha: Option<&'static str>,
    cargo_target_triple: Option<&'static str>,
    cargo_features: Option<&'static str>,
    cargo_opt_level: Option<&'static str>,
}

impl BuildInfo {
    pub fn new(ctx: &Context<'_>) -> Self {
        let build_info = from_catalog_n!(ctx, observability::build_info::BuildInfo);
        Self {
            app_version: build_info.app_version,
            build_timestamp: build_info.build_timestamp,
            git_describe: build_info.git_describe,
            git_sha: build_info.git_sha,
            git_commit_date: build_info.git_commit_date,
            git_branch: build_info.git_branch,
            rustc_semver: build_info.rustc_semver,
            rustc_channel: build_info.rustc_channel,
            rustc_host_triple: build_info.rustc_host_triple,
            rustc_commit_sha: build_info.rustc_commit_sha,
            cargo_target_triple: build_info.cargo_target_triple,
            cargo_features: build_info.cargo_features,
            cargo_opt_level: build_info.cargo_opt_level,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
