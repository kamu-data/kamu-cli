// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(setty::Config, setty::Default)]
pub struct VersionedFilesConfig {
    /// Largest file version, in bytes, whose content is returned in-band in a
    /// GraphQL response. Larger files are downloaded through their content URL
    #[config(default = VersionedFilesConfig::DEFAULT_MAX_IN_BAND_CONTENT_SIZE_IN_BYTES)]
    pub max_in_band_content_size_in_bytes: usize,
}

impl VersionedFilesConfig {
    pub const DEFAULT_MAX_IN_BAND_CONTENT_SIZE_IN_BYTES: usize = 10 * 1024 * 1024;

    pub fn new_in_bytes(max_in_band_content_size_in_bytes: usize) -> Self {
        Self {
            max_in_band_content_size_in_bytes,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
