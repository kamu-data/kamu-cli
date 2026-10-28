// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockLocalSyncCommitter {
    /// Expects one commit of a transfer up to `new_head`, answering with
    /// `result`
    pub fn make_expect_commit(
        mut self,
        new_head: odf::Multihash,
        result: Result<SyncResult, SyncError>,
    ) -> Self {
        let result = Mutex::new(Some(result));
        self.expect_commit()
            .withf(move |transferred| transferred.new_head == new_head)
            .times(1)
            .returning(move |_| result.lock().unwrap().take().unwrap());
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
