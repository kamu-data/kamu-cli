// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Into;

use async_graphql::*;
use kamu_core::StatusCheckError;
use kamu_core::utils::metadata_chain_comparator as comp;

use crate::scalars::DatasetRefRemote;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetPushStatuses
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sync state of a dataset relative to one of its push remotes
#[derive(SimpleObject, Debug)]
pub struct DatasetPushStatus {
    /// Remote dataset the local one is pushed to
    pub remote: DatasetRefRemote<'static>,
    /// Result of comparing the remote metadata chain with the local one
    pub result: CompareChainsResult,
}

/// Sync states of a dataset relative to all its push remotes
#[derive(SimpleObject, Debug)]
pub struct DatasetPushStatuses {
    /// Sync state per push remote
    pub statuses: Vec<DatasetPushStatus>,
}

impl From<kamu_core::DatasetPushStatuses> for DatasetPushStatuses {
    fn from(value: kamu_core::DatasetPushStatuses) -> Self {
        let statuses: Vec<DatasetPushStatus> = value
            .statuses
            .into_iter()
            .map(
                |kamu_core::PushStatus {
                     remote,
                     check_result,
                 }| DatasetPushStatus {
                    remote: remote.into(),
                    result: check_result.into(),
                },
            )
            .collect();
        Self { statuses }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CompareChainsResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reason the metadata chains could not be compared
#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultReason {
    /// Description of the problem
    pub message: String,
}

/// Metadata chains could not be compared
#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultError {
    /// Reason of the failure
    pub reason: CompareChainsResultReason,
}

/// Metadata chains were compared successfully
#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultStatus {
    /// State of the remote chain relative to the local one
    pub message: CompareChainsStatus,
}

/// State of a remote metadata chain relative to the local one
#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum CompareChainsStatus {
    /// Both chains have the same head
    Equal,
    /// Remote is missing blocks the local chain has
    Behind,
    /// Remote has blocks the local chain is missing
    Ahead,
    /// Chains have diverged or belong to different datasets
    Diverged,
}

/// Outcome of comparing a remote metadata chain with the local one
#[derive(Union, Debug)]
pub enum CompareChainsResult {
    Status(CompareChainsResultStatus),
    Error(CompareChainsResultError),
}

impl From<Result<comp::CompareChainsResult, StatusCheckError>> for CompareChainsResult {
    fn from(value: Result<comp::CompareChainsResult, StatusCheckError>) -> Self {
        match value {
            Ok(comp::CompareChainsResult::Equal) => {
                CompareChainsResult::Status(CompareChainsResultStatus {
                    message: CompareChainsStatus::Equal,
                })
            }
            Ok(comp::CompareChainsResult::LhsAhead { .. }) => {
                CompareChainsResult::Status(CompareChainsResultStatus {
                    message: CompareChainsStatus::Behind,
                })
            }
            Ok(comp::CompareChainsResult::LhsBehind { .. }) => {
                CompareChainsResult::Status(CompareChainsResultStatus {
                    message: CompareChainsStatus::Ahead,
                })
            }
            Ok(
                comp::CompareChainsResult::Divergence { .. }
                | comp::CompareChainsResult::DifferentDatasets { .. },
            ) => CompareChainsResult::Status(CompareChainsResultStatus {
                message: CompareChainsStatus::Diverged,
            }),
            Err(e) => CompareChainsResult::Error(CompareChainsResultError {
                reason: CompareChainsResultReason {
                    message: e.to_string(),
                },
            }),
        }
    }
}
