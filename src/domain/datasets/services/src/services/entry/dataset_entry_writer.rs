// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::{DatasetEntryNameCollisionError, SaveDatasetEntryErrorDuplicate};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetEntryWriter: Send + Sync {
    async fn create_entry(
        &self,
        dataset_id: &odf::DatasetID,
        owner_account_id: &odf::AccountID,
        owner_account_name: &odf::AccountName,
        dataset_name: &odf::DatasetName,
        dataset_kind: odf::DatasetKind,
    ) -> Result<(), CreateDatasetEntryError>;

    async fn rename_entry(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_dataset_name: &odf::DatasetName,
    ) -> Result<(), RenameDatasetEntryError>;

    async fn remove_entry(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), InternalError>;

    async fn update_owner_entries_after_rename(
        &self,
        owner_account_id: &odf::AccountID,
        old_owner_account_name: &odf::AccountName,
        new_owner_account_name: &odf::AccountName,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CreateDatasetEntryError {
    #[error(transparent)]
    DuplicateId(#[from] SaveDatasetEntryErrorDuplicate),

    #[error(transparent)]
    NameCollision(#[from] DatasetEntryNameCollisionError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RenameDatasetEntryError {
    #[error(transparent)]
    NameCollision(#[from] DatasetEntryNameCollisionError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
