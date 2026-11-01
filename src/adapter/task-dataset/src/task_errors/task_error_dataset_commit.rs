// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_error_enum! {
    /// The task's dataset change could not be committed; a retry plans it again
    pub enum TaskErrorDatasetCommit {
        HeadMoved(DatasetHeadMovedError),
    }
    => "DatasetCommitError", recoverable: true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` was no longer at the head the task had planned from
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetHeadMovedError {
    pub dataset_id: odf::DatasetID,
    pub expected_head: Option<odf::Multihash>,
    pub actual_head: Option<odf::Multihash>,
}

impl DatasetHeadMovedError {
    /// `heads` are `None` when the destination did not report them
    pub fn new(dataset_id: odf::DatasetID, heads: Option<odf::dataset::RefCASError>) -> Self {
        let (expected_head, actual_head) = heads.map_or((None, None), |e| (e.expected, e.actual));
        Self {
            dataset_id,
            expected_head,
            actual_head,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
