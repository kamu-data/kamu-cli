// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PullItem {
    pub(super) depth: i32,
    pub(super) local_target: PullLocalTarget,
    pub(super) kind: DatasetUpdateKind,
    pub(super) maybe_original_request: Option<PullRequest>,
}

impl PartialOrd for PullItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PullItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let depth_ord = self.depth.cmp(&other.depth);
        if depth_ord != Ordering::Equal {
            return depth_ord;
        }

        if self.kind.is_sync_from() != other.kind.is_sync_from() {
            return if self.kind.is_sync_from() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        self.local_target.alias().cmp(other.local_target.alias())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
