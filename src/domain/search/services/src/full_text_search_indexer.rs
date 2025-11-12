// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_datasets::JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER;
use kamu_search::FullTextSearchRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.FullTextSearchIndexer",
    depends_on: &[JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER],
    requires_transaction: true,
})]
pub struct FullTextSearchIndexer {
    #[allow(dead_code)]
    full_text_repo: Arc<dyn FullTextSearchRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for FullTextSearchIndexer {
    #[tracing::instrument(level = "info", skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // TODO: indexing logic
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
