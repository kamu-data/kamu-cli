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
use reqwest::Url;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ServerSideHarness {
    fn dataset_repository(&self) -> Arc<dyn DatasetRepository>;

    fn dataset_layout(&self, dataset_name: &str) -> DatasetLayout;

    fn dataset_url(&self, dataset_name: &str) -> Url;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
