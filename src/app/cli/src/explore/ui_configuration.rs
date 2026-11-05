// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UIConfiguration {
    pub(crate) ingest_upload_file_limit_mb: usize,
    pub(crate) min_new_password_length: usize,
    pub(crate) feature_flags: UIFeatureFlags,
    pub(crate) webhooks: UIWebhooksConfiguration,
    pub(crate) versioned_files: UIVersionedFilesConfiguration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UIFeatureFlags {
    pub(crate) enable_logout: bool,
    pub(crate) enable_scheduling: bool,
    pub(crate) enable_dataset_env_vars_management: bool,
    pub(crate) enable_terms_of_service: bool,
    pub(crate) allow_anonymous: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UIWebhooksConfiguration {
    pub(crate) loopback_targets_only: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UIVersionedFilesConfiguration {
    /// Files up to this size can be read in-band through
    /// `VersionedFileEntry.content`, larger ones through `contentUrl`
    pub(crate) max_in_band_content_size_in_bytes: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
