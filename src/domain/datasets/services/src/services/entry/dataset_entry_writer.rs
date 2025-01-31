// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetEntryWriter: Send + Sync {
    async fn create_entry(
        &self,
        dataset_id: &odf::DatasetID,
        owner_account_id: &odf::AccountID,
        dataset_name: &odf::DatasetName,
    ) -> Result<(), InternalError>;

    async fn rename_entry(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_dataset_name: &odf::DatasetName,
    ) -> Result<(), InternalError>;

    async fn remove_entry(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
