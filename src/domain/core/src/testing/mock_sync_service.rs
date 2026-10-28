// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ReportedHeads;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockSyncService {
    pub const REPORTED_INCREMENT: odf::dataset::MetadataChainIncrementInterval =
        odf::dataset::MetadataChainIncrementInterval {
            num_blocks: 1,
            num_records: 7,
            updated_watermark: None,
        };

    /// Expects one transfer from `src_remote_ref` into the existing local
    /// `target_alias`. It is up to date without `reported_heads`, otherwise it
    /// transfers up to their new head
    pub fn make_expect_transfer_from_remote_into_existing_local(
        mut self,
        target_alias: odf::DatasetAlias,
        src_remote_ref: odf::DatasetRefRemote,
        reported_heads: Option<ReportedHeads>,
    ) -> Self {
        self.expect_transfer_into_local()
            .withf(move |request, _, _| {
                request.src == src_remote_ref
                    && matches!(
                        &(request.dst),
                        LocalSyncTarget::Existing { dataset, .. }
                        if dataset.get_alias() == &target_alias
                    )
            })
            .times(1)
            .returning(move |request, _, _| {
                let Some(reported_heads) = &reported_heads else {
                    return Ok(SyncTransfer::UpToDate);
                };
                let LocalSyncTarget::Existing {
                    dataset,
                    expected_head,
                } = request.dst
                else {
                    unreachable!()
                };
                Ok(SyncTransfer::Transferred(TransferredSync {
                    target: TransferredSyncTarget::Existing {
                        dataset,
                        expected_head,
                    },
                    new_head: reported_heads.get().1,
                    num_blocks: 1,
                    increment: Self::REPORTED_INCREMENT,
                }))
            });
        self
    }

    /// Expects one transfer into the existing local `target_alias`, failing
    /// with `make_error()`
    pub fn make_expect_transfer_error(
        mut self,
        target_alias: odf::DatasetAlias,
        make_error: impl Fn() -> SyncError + Send + 'static,
    ) -> Self {
        self.expect_transfer_into_local()
            .withf(move |request, _, _| {
                matches!(
                    &(request.dst),
                    LocalSyncTarget::Existing { dataset, .. }
                    if dataset.get_alias() == &target_alias
                )
            })
            .times(1)
            .returning(move |_, _, _| Err(make_error()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
