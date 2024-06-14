// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{AccountID, DatasetID};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetOwnershipService: Sync + Send {
    async fn get_dataset_owners(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Vec<AccountID>, InternalError>;

    async fn get_owned_datasets(
        &self,
        account_id: &AccountID,
    ) -> Result<Vec<DatasetID>, InternalError>;

    async fn is_dataset_owned_by(
        &self,
        dataset_id: &DatasetID,
        account_id: &AccountID,
    ) -> Result<bool, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
