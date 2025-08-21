// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_datasets::JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DEPENDENCY_GRAPH_INDEXER,
    depends_on: &[],
    requires_transaction: true,
})]
pub struct FakeDependencyGraphIndexer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for FakeDependencyGraphIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FakeDependencyGraphIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Be silent
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
