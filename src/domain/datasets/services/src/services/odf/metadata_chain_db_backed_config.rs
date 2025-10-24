// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct MetadataChainDbBackedConfig {
    pub data_blocks_page_size: usize,
}

impl Default for MetadataChainDbBackedConfig {
    fn default() -> Self {
        Self {
            data_blocks_page_size: 100,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
