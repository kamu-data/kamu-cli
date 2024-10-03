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

    async fn is_action_allowed(
        &self,
        dataset_handle: &DatasetHandle,
        action: DatasetAction,
    ) -> Result<bool, InternalError> {
        match self.check_action_allowed(dataset_handle, action).await {
            Ok(()) => Ok(true),
            Err(DatasetActionUnauthorizedError::Access(_)) => Ok(false),
            Err(DatasetActionUnauthorizedError::Internal(err)) => Err(err),
        }
    }

    async fn get_allowed_actions(&self, dataset_handle: &DatasetHandle) -> HashSet<DatasetAction>;

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        action: DatasetAction,
    ) -> Result<Vec<DatasetHandle>, InternalError>;

    async fn classify_datasets_by_allowance(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError>;
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

#[derive(Debug)]
pub struct ClassifyByAllowanceResponse {
    pub authorized_handles: Vec<DatasetHandle>,
    pub unauthorized_handles_with_errors: Vec<(DatasetHandle, DatasetActionUnauthorizedError)>,
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

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        _action: DatasetAction,
    ) -> Result<Vec<DatasetHandle>, InternalError> {
        Ok(dataset_handles)
    }

    async fn classify_datasets_by_allowance(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        _action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        Ok(ClassifyByAllowanceResponse {
            authorized_handles: dataset_handles,
            unauthorized_handles_with_errors: vec![],
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
