// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::*;
use kamu_datasets::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub SyncService {}

    #[async_trait::async_trait]
    impl SyncService for SyncService {
        async fn sync(
            &self,
            request: SyncRequest,
            options: SyncOptions,
            listener: Option<Arc<dyn SyncListener>>,
        ) -> Result<SyncResult, SyncError>;

        /// Adds dataset to IPFS and returns the root CID.
        /// Unlike `sync` it does not do IPNS resolution and publishing.
        async fn ipfs_add(&self, src: ResolvedDataset) -> Result<String, IpfsAddError>;
    }
}

impl MockSyncService {
    pub fn make_expect_sync_pull_from_remote_to_existing_local(
        mut self,
        target_alias: odf::DatasetAlias,
        src_remote_ref: odf::DatasetRefRemote,
        injected_result: SyncResult,
    ) -> Self {
        self.expect_sync()
            .withf(move |request, _, _| {
                matches!(
                    &(request.src),
                    SyncRef::Remote(SyncRefRemote {
                        url: _,
                        dataset: _,
                        original_remote_ref,
                    })
                    if original_remote_ref == &src_remote_ref
                ) && matches!(
                    &(request.dst),
                    SyncRef::Local(resolved_dataset)
                    if resolved_dataset.get_alias() == &target_alias
                )
            })
            .times(1)
            .returning(move |_, _, _| Ok(injected_result.clone()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
