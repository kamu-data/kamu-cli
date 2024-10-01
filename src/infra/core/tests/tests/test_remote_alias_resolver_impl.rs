// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dill::*;
use futures::lock::Mutex;
use kamu::{
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    PushServiceImpl,
    RemoteAliasesRegistryImpl,
    RemoteRepositoryRegistryImpl,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::{self, AlwaysHappyDatasetActionAuthorizer};
use kamu_core::{
    DatasetRepository,
    PollingIngestService,
    PushService,
    RemoteAliasesRegistry,
    RemoteRepositoryRegistry,
    SyncService,
};
use time_source::SystemTimeSourceDefault;

#[tokio::test]
async fn test_set_watermark_rejects_on_derivative() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let harness = RemoteAliasResolverHarness::new_with_authorizer(
        tmp_dir.path(),
        AlwaysHappyDatasetActionAuthorizer::new(),
        true,
    );
}

struct RemoteAliasResolverHarness {
    dataset_repo: Arc<DatasetRepositoryLocalFs>,
    remote_repo_reg: Arc<RemoteRepositoryRegistryImpl>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    push_svc: Arc<dyn PushService>,
}

impl RemoteAliasResolverHarness {
    fn new(tmp_path: &PathBuf, multi_tenant: bool) -> Self {
        Self::new_with_authorizer(
            tmp_path,
            AlwaysHappyDatasetActionAuthorizer::new(),
            multi_tenant,
        )
    }

    fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        tmp_path: &Path,
        dataset_action_authorizer: TDatasetAuthorizer,
        multi_tenant: bool,
    ) -> Self {
        let datasets_dir_path = tmp_path.join("datasets");
        std::fs::create_dir(&datasets_dir_path).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir_path)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(RemoteRepositoryRegistryImpl::create(tmp_path.join("repos")).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            // .add_builder(TestSyncService::builder().with_calls(calls.clone()))
            // .bind::<dyn SyncService, TestSyncService>()
            .add::<PushServiceImpl>()
            .build();

        Self {
            dataset_repo: catalog.get_one().unwrap(),
            remote_repo_reg: catalog.get_one().unwrap(),
            remote_alias_reg: catalog.get_one().unwrap(),
            push_svc: catalog.get_one().unwrap(),
        }
    }
}
