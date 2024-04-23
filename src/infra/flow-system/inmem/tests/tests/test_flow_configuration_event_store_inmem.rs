// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_flow_system_inmem::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_store_empty() {
    let catalog = CatalogBuilder::new()
        .add::<FlowConfigurationEventStoreInMem>()
        .build();

    kamu_flow_system_repo_tests::test_event_store_empty(&catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
