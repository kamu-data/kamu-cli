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
use kamu_core::utils::metadata_chain_comparator as comp;
use kamu_core::StatusCheckError;

use crate::scalars::DatasetRefRemote;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetPushStatuses
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct DatasetPushStatus {
    pub remote: DatasetRefRemote<'static>,
    pub result: CompareChainsResult,
}

#[derive(SimpleObject, Debug)]
pub struct DatasetPushStatuses {
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

#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultReason {
    pub message: String,
}

#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultError {
    pub reason: CompareChainsResultReason,
}

#[derive(SimpleObject, Debug)]
pub struct CompareChainsResultStatus {
    pub message: CompareChainsStatus,
}

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum CompareChainsStatus {
    Equal,
    Behind,
    Ahead,
    Diverged,
}

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
            Ok(comp::CompareChainsResult::Divergence { .. }) => {
                CompareChainsResult::Status(CompareChainsResultStatus {
                    message: CompareChainsStatus::Diverged,
                })
            }
            Err(e) => CompareChainsResult::Error(CompareChainsResultError {
                reason: CompareChainsResultReason {
                    message: e.to_string(),
                },
            }),
        }
    }
}
