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
use opendatafabric::{DatasetRef, DatasetRefAny, DatasetRefRemote};
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ClientSideHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    pull_service: Arc<dyn PullService>,
    push_service: Arc<dyn PushService>,
}

impl ClientSideHarness {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();

        let mut b = dill::CatalogBuilder::new();

        b.add_builder(
            builder_for::<DatasetRepositoryLocalFs>()
                .with_root(workspace_layout.datasets_dir.clone()),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();

        b.add_builder(
            builder_for::<RemoteRepositoryRegistryImpl>()
                .with_repos_dir(workspace_layout.repos_dir.clone()),
        )
        .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>();

        b.add_builder(
            builder_for::<RemoteAliasesRegistryImpl>()
                .with_datasets_dir(workspace_layout.datasets_dir.clone()),
        )
        .bind::<dyn RemoteAliasesRegistry, RemoteAliasesRegistryImpl>();

        b.add_value(EngineProvisionerNull)
            .bind::<dyn EngineProvisioner, EngineProvisionerNull>();

        b.add_builder(
            builder_for::<IngestServiceImpl>()
                .with_run_info_dir(workspace_layout.run_info_dir.clone())
                .with_cache_dir(workspace_layout.datasets_dir.clone()),
        )
        .bind::<dyn IngestService, IngestServiceImpl>();

        b.add::<DatasetFactoryImpl>()
            .bind::<dyn DatasetFactory, DatasetFactoryImpl>();

        b.add::<SmartTransferProtocolClientWs>()
            .bind::<dyn SmartTransferProtocolClient, SmartTransferProtocolClientWs>();

        b.add::<SyncServiceImpl>()
            .bind::<dyn SyncService, SyncServiceImpl>();

        b.add_builder(
            builder_for::<TransformServiceImpl>()
                .with_run_info_dir(workspace_layout.run_info_dir.clone()),
        )
        .bind::<dyn TransformService, TransformServiceImpl>();

        b.add::<PullServiceImpl>()
            .bind::<dyn PullService, PullServiceImpl>();

        b.add::<PushServiceImpl>()
            .bind::<dyn PushService, PushServiceImpl>();

        b.add_value(workspace_layout.clone())
            .add_value(ContainerRuntime::default())
            .add_value(kamu::utils::ipfs_wrapper::IpfsClient::default())
            .add_value(IpfsGateway::default());

        let catalog = b.build();

        let pull_service = catalog.get_one::<dyn PullService>().unwrap();
        let push_service = catalog.get_one::<dyn PushService>().unwrap();

        Self {
            tempdir,
            catalog,
            pull_service,
            push_service,
        }
    }

    pub fn dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        self.catalog.get_one::<dyn DatasetRepository>().unwrap()
    }

    pub fn dataset_layout(&self, dataset_name: &str) -> DatasetLayout {
        DatasetLayout::new(
            self.internal_datasets_folder_path()
                .join(dataset_name)
                .as_path(),
        )
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
