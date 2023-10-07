// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use kamu::domain::{DatasetRepository, InternalError};
use kamu::DatasetLayout;
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle};
use reqwest::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub const SERVER_ACCOUNT_NAME: &str = "kamu-server";

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ServerSideHarness {
    fn operating_account_name(&self) -> Option<AccountName>;

    fn cli_dataset_repository(&self) -> Arc<dyn DatasetRepository>;

    fn dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout;

    fn dataset_url(&self, dataset_alias: &DatasetAlias) -> Url;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
