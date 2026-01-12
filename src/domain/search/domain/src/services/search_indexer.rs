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
    // Whether to clear and re-index on start or work with existing vectors is any exist
    pub clear_on_start: bool,

    /// Whether to skip indexing datasets that have no readme or description
    pub skip_datasets_with_no_description: bool,

    /// Whether to skip indexing datasets that have no data
    pub skip_datasets_with_no_data: bool,

    /// Whether to include the original text as payload of the vectors when
    /// storing them. It is not needed for normal service operations but can
    /// help debug issues.
    pub payload_include_content: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
