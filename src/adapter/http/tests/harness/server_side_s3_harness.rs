// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use internal_error::InternalError;
use kamu::*;
use kamu_accounts_services::ModifyAccountPasswordUseCaseImpl;
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    DatasetReferenceUpdaterImpl,
    DatasetS3BuilderDatabaseBackedImpl,
};
use test_utils::LocalS3Server;

use super::{
    BaseServerSideHarness,
    S3DatasetFixture,
    ServerSideDatasetFixture,
    ServerSideHarness,
    ServerSideHarnessOptions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideS3Harness {
    base: BaseServerSideHarness,
    s3: LocalS3Server,
    _temp_dir: tempfile::TempDir,
}

impl ServerSideS3Harness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let s3 = LocalS3Server::new().await;
        s3.set_credentials_env_vars();

        let base = BaseServerSideHarness::new(options, run_info_dir, |b| {
            b.add_builder(odf::dataset::DatasetStorageUnitS3::builder(s3.ctx.clone()))
                .add::<DatasetS3BuilderDatabaseBackedImpl>()
                .add_builder(ObjectStoreBuilderS3::builder(
                    s3.ctx.clone(),
                    true,
                    Some(s3.access_key.clone()),
                    Some(s3.secret_key.clone()),
                ))
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<DatasetReferenceUpdaterImpl>()
                .add::<ModifyAccountPasswordUseCaseImpl>();

            NoOpDatabasePlugin::init_database_components(b);
        })
        .await;

        Self {
            base,
            s3,
            _temp_dir: temp_dir,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.s3.bucket
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl ServerSideHarness for ServerSideS3Harness {
    fn base(&self) -> &BaseServerSideHarness {
        &self.base
    }

    fn dataset_fixture(&self) -> Arc<dyn ServerSideDatasetFixture> {
        Arc::new(S3DatasetFixture::new(self.s3.ctx.clone()))
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.base.api_server_run().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
