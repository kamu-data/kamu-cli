// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetOwnershipService: Sync + Send {
    async fn get_dataset_owner(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<odf::AccountID, InternalError>;

    async fn is_dataset_owned_by(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
    ) -> Result<bool, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
