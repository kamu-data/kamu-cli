// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;

use super::protocol::SimpleTransferProtocol;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
    pub(super) fn report_blocks_written(
        listener: &dyn SyncListener,
        mut stats: SyncStats,
        num_blocks: u64,
    ) {
        stats.dst.metadata_blocks_written += num_blocks;
        listener.on_status(SyncStage::CommitBlocks, &stats);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct CompareChainsListenerAdapter {
    l: Arc<dyn SyncListener>,
    stats: Mutex<SyncStats>,
}

impl CompareChainsListenerAdapter {
    pub(super) fn new(l: Arc<dyn SyncListener>) -> Self {
        Self {
            l,
            stats: Mutex::new(SyncStats::default()),
        }
    }

    pub(super) fn into_status(self) -> SyncStats {
        self.stats.into_inner().unwrap()
    }
}

impl CompareChainsListener for CompareChainsListenerAdapter {
    fn on_lhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_lhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
