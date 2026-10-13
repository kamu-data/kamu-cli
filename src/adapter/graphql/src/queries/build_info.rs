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

/// Build details of the running server; fields are null when unknown
#[derive(async_graphql::SimpleObject)]
pub struct BuildInfo {
    /// Version of the application
    app_version: &'static str,
    /// Time the binary was built
    build_timestamp: Option<&'static str>,
    /// Output of `git describe` for the built revision
    git_describe: Option<&'static str>,
    /// Git commit hash the binary was built from
    git_sha: Option<&'static str>,
    /// Date of the git commit the binary was built from
    git_commit_date: Option<&'static str>,
    /// Git branch the binary was built from
    git_branch: Option<&'static str>,
    /// Version of the Rust compiler
    rustc_semver: Option<&'static str>,
    /// Release channel of the Rust compiler
    rustc_channel: Option<&'static str>,
    /// Host target triple of the Rust compiler
    rustc_host_triple: Option<&'static str>,
    /// Commit hash of the Rust compiler
    rustc_commit_sha: Option<&'static str>,
    /// Target triple the binary was built for
    cargo_target_triple: Option<&'static str>,
    /// Cargo features enabled in the build
    cargo_features: Option<&'static str>,
    /// Compiler optimization level of the build
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
