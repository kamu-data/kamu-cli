// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use container_runtime::ContainerRuntime;
use dill::builder_for;
use kamu::domain::*;
use kamu::utils::smart_transfer_protocol::SmartTransferProtocolClient;
use kamu::*;
use kamu_adapter_http::SmartTransferProtocolClientWs;
use opendatafabric::{AccountName, DatasetID, DatasetRef, DatasetRefAny, DatasetRefRemote};
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

const CLIENT_ACCOUNT_NAME: &str = "kamu-client";

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ClientSideHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    pull_service: Arc<dyn PullService>,
    push_service: Arc<dyn PushService>,
    options: ClientSideHarnessOptions,
}

pub(crate) struct ClientSideHarnessOptions {
    pub multi_tenant: bool,
    pub authenticated_remotely: bool,
}

impl ClientSideHarness {
    pub fn new(options: ClientSideHarnessOptions) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();
        let repos_dir = tempdir.path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();
        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();
        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let mut b = dill::CatalogBuilder::new();

        b.add_value(CurrentAccountSubject::logged(AccountName::new_unchecked(
            CLIENT_ACCOUNT_NAME,
        )));

        b.add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>();

        if options.authenticated_remotely {
            b.add::<auth::DummyOdfServerAccessTokenResolver>();
            b.bind::<dyn auth::OdfServerAccessTokenResolver, auth::DummyOdfServerAccessTokenResolver>();
        } else {
            b.add_value(kamu::testing::MockOdfServerAccessTokenResolver::empty());
            b.bind::<dyn auth::OdfServerAccessTokenResolver, kamu::testing::MockOdfServerAccessTokenResolver>();
        }

        b.add::<SystemTimeSourceDefault>();
        b.bind::<dyn SystemTimeSource, SystemTimeSourceDefault>();

        b.add_builder(
            builder_for::<DatasetRepositoryLocalFs>()
                .with_root(datasets_dir)
                .with_multi_tenant(options.multi_tenant),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();

        b.add_builder(builder_for::<RemoteRepositoryRegistryImpl>().with_repos_dir(repos_dir))
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>();

        b.add::<RemoteAliasesRegistryImpl>()
            .bind::<dyn RemoteAliasesRegistry, RemoteAliasesRegistryImpl>();

        b.add_value(EngineProvisionerNull)
            .bind::<dyn EngineProvisioner, EngineProvisionerNull>();

        b.add::<ObjectStoreRegistryImpl>()
            .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>();

        b.add::<DataFormatRegistryImpl>();
        b.bind::<dyn DataFormatRegistry, DataFormatRegistryImpl>();

        b.add_builder(
            builder_for::<PollingIngestServiceImpl>()
                .with_run_info_dir(run_info_dir)
                .with_cache_dir(cache_dir),
        )
        .bind::<dyn PollingIngestService, PollingIngestServiceImpl>();

        b.add::<DatasetFactoryImpl>()
            .bind::<dyn DatasetFactory, DatasetFactoryImpl>();

        b.add::<SmartTransferProtocolClientWs>()
            .bind::<dyn SmartTransferProtocolClient, SmartTransferProtocolClientWs>();

        b.add::<SyncServiceImpl>()
            .bind::<dyn SyncService, SyncServiceImpl>();

        b.add::<TransformServiceImpl>()
            .bind::<dyn TransformService, TransformServiceImpl>();

        b.add::<PullServiceImpl>()
            .bind::<dyn PullService, PullServiceImpl>();

        b.add::<PushServiceImpl>()
            .bind::<dyn PushService, PushServiceImpl>();

        b.add_value(ContainerRuntime::default());
        b.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());
        b.add_value(IpfsGateway::default());

        let catalog = b.build();

        let pull_service = catalog.get_one::<dyn PullService>().unwrap();
        let push_service = catalog.get_one::<dyn PushService>().unwrap();

        Self {
            tempdir,
            catalog,
            pull_service,
            push_service,
            options,
        }
    }

    pub fn operating_account_name(&self) -> Option<AccountName> {
        if self.options.multi_tenant && self.options.authenticated_remotely {
            Some(AccountName::new_unchecked(CLIENT_ACCOUNT_NAME))
        } else {
            None
        }
    }

    pub fn dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        self.catalog.get_one::<dyn DatasetRepository>().unwrap()
    }

    // TODO: accept alias or handle
    pub fn dataset_layout(&self, dataset_id: &DatasetID, dataset_name: &str) -> DatasetLayout {
        let root_path = if self.options.multi_tenant {
            self.internal_datasets_folder_path()
                .join(CLIENT_ACCOUNT_NAME)
                .join(dataset_id.cid.to_string())
        } else {
            self.internal_datasets_folder_path().join(dataset_name)
        };
        DatasetLayout::new(root_path.as_path())
    }

    pub async fn pull_datasets(&self, dataset_ref: DatasetRefAny) -> Vec<PullResponse> {
        self.pull_service
            .pull_multi(
                vec![dataset_ref],
                PullMultiOptions {
                    sync_options: SyncOptions {
                        create_if_not_exists: true,
                        ..SyncOptions::default()
                    },
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
    }

    pub async fn pull_dataset_result(&self, dataset_ref: DatasetRefAny) -> PullResult {
        self.pull_datasets(dataset_ref)
            .await
            .get(0)
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .clone()
    }

    pub async fn push_dataset(
        &self,
        dataset_local_ref: DatasetRef,
        dataset_remote_ref: DatasetRefRemote,
    ) -> Vec<PushResponse> {
        self.push_service
            .push_multi_ext(
                vec![PushRequest {
                    local_ref: Some(dataset_local_ref),
                    remote_ref: Some(dataset_remote_ref),
                }],
                PushMultiOptions {
                    sync_options: SyncOptions {
                        create_if_not_exists: true,
                        ..SyncOptions::default()
                    },
                    ..PushMultiOptions::default()
                },
                None,
            )
            .await
    }

    pub async fn push_dataset_result(
        &self,
        dataset_local_ref: DatasetRef,
        dataset_remote_ref: DatasetRefRemote,
    ) -> SyncResult {
        let results = self
            .push_dataset(dataset_local_ref, dataset_remote_ref)
            .await;

        match &(results.get(0).unwrap().result) {
            Ok(sync_result) => sync_result.clone(),
            Err(e) => {
                println!("Error: {:#?}", e);
                panic!("Failure")
            }
        }
    }

    pub fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
