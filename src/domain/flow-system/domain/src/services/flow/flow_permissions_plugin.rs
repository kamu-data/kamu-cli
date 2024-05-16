// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::InternalError;
use opendatafabric::{AccountID, DatasetID};

#[async_trait::async_trait]
pub trait FlowPermissionsPlugin: Sync + Send {
    /// Get list of downstream dependencies filtered by account_id
    async fn get_account_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
        manual_trigger_maybe: Option<AccountID>,
    ) -> Result<Vec<DatasetID>, InternalError>;
}
