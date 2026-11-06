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

/// A dataset in the pull graph, with the job that updates it and its depth.
/// Items order by depth, syncs first within a depth, then by local alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PullItem {
    pub depth: i32,
    pub job: PullItemJob,
    /// The request that named the dataset; `None` for an upstream reached
    /// while traversing
    pub maybe_original_request: Option<PullRequest>,
}

impl PartialOrd for PullItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PullItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.depth
            .cmp(&other.depth)
            .then_with(|| other.job.is_sync().cmp(&self.job.is_sync()))
            .then_with(|| self.job.local_alias().cmp(other.job.local_alias()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a dataset in the pull graph is updated. Only a sync can create its
/// dataset; an ingest or a transform updates an existing one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PullItemJob {
    Ingest(odf::DatasetHandle),
    Transform(odf::DatasetHandle),
    Sync {
        local_target: PullLocalTarget,
        remote_ref: odf::DatasetRefRemote,
    },
}

impl PullItemJob {
    fn local_alias(&self) -> &odf::DatasetAlias {
        match self {
            Self::Ingest(handle) | Self::Transform(handle) => &handle.alias,
            Self::Sync { local_target, .. } => local_target.alias(),
        }
    }

    fn is_sync(&self) -> bool {
        match self {
            Self::Ingest(_) | Self::Transform(_) => false,
            Self::Sync { .. } => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
