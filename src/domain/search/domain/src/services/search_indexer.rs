// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SearchIndexer: Send + Sync {
    async fn reset_search_indices(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Copy, Clone)]
pub struct SearchIndexerConfig {
    // Whether to clear and re-index on start or work with existing data if any exists
    pub clear_on_start: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
