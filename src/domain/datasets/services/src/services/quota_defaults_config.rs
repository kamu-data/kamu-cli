// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub const DEFAULT_STORAGE_QUOTA_BYTES: u64 = 1_000_000_000;

#[derive(Debug, Clone)]
pub struct QuotaDefaultsConfig {
    pub storage: u64,
}

impl Default for QuotaDefaultsConfig {
    fn default() -> Self {
        Self {
            storage: DEFAULT_STORAGE_QUOTA_BYTES,
        }
    }
}
