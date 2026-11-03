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
    /// The reset cannot be made; its heads come from the flow's configuration,
    /// so a retry fails the same way
    pub enum TaskErrorDatasetReset {
        ResetHeadNotFound,
        OldHeadMismatch(DatasetResetOldHeadMismatch),
    }
    => "ResetDatasetError", recoverable: false
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` is not the head the reset expects
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetResetOldHeadMismatch {
    pub expected_head: odf::Multihash,
    pub actual_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
