// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::str::FromStr;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError};
use opendatafabric::{DatasetHandle, DatasetRef};
use thiserror::Error;

use crate::AccessError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetActionAuthorizer: Sync + Send {
    async fn check_action_allowed(
        &self,
        dataset_handle: &DatasetHandle,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError>;

    async fn get_allowed_actions(&self, dataset_handle: &DatasetHandle) -> HashSet<DatasetAction>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum DatasetAction {
    Read,
    Write,
}

impl FromStr for DatasetAction {
    type Err = InternalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "read" {
            Ok(DatasetAction::Read)
        } else if s == "write" {
            Ok(DatasetAction::Write)
        } else {
            Err(format!("Invalid DatasetAction: {s}").int_err())
        }
    }
}

impl std::fmt::Display for DatasetAction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatasetAction::Read => write!(f, "read"),
            DatasetAction::Write => write!(f, "write"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DatasetActionUnauthorizedError {
    #[error(transparent)]
    Access(AccessError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("User has no '{action}' permission in dataset '{dataset_ref}'")]
pub struct DatasetActionNotEnoughPermissionsError {
    pub action: DatasetAction,
    pub dataset_ref: DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetActionAuthorizer)]
pub struct AlwaysHappyDatasetActionAuthorizer {}

impl AlwaysHappyDatasetActionAuthorizer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl DatasetActionAuthorizer for AlwaysHappyDatasetActionAuthorizer {
    async fn check_action_allowed(
        &self,
        _dataset_handle: &DatasetHandle,
        _action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        // Ignore rules
        Ok(())
    }

    async fn get_allowed_actions(&self, _dataset_handle: &DatasetHandle) -> HashSet<DatasetAction> {
        HashSet::from([DatasetAction::Read, DatasetAction::Write])
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
