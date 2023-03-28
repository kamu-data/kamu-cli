// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{path::PathBuf, sync::Arc};

use container_runtime::ContainerRuntime;
use kamu::{
    domain::*,
    infra::{utils::smart_transfer_protocol::SmartTransferProtocolClient, *},
};
use kamu_adapter_http::WsSmartTransferProtocolClient;
use opendatafabric::DatasetRefAny;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ClientSideHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    pull_service: Arc<dyn PullService>,
}

impl ClientSideHarness {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();

        let mut b = dill::CatalogBuilder::new();

        b.add::<DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();

        b.add::<RemoteRepositoryRegistryImpl>()
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>();

        b.add::<RemoteAliasesRegistryImpl>()
            .bind::<dyn RemoteAliasesRegistry, RemoteAliasesRegistryImpl>();

        b.add_value(EngineProvisionerNull)
            .bind::<dyn EngineProvisioner, EngineProvisionerNull>();

        b.add::<IngestServiceImpl>()
            .bind::<dyn IngestService, IngestServiceImpl>();

        b.add::<DatasetFactoryImpl>()
            .bind::<dyn DatasetFactory, DatasetFactoryImpl>();

        b.add::<WsSmartTransferProtocolClient>()
            .bind::<dyn SmartTransferProtocolClient, WsSmartTransferProtocolClient>();

        b.add::<SyncServiceImpl>()
            .bind::<dyn SyncService, SyncServiceImpl>();

        b.add::<TransformServiceImpl>()
            .bind::<dyn TransformService, TransformServiceImpl>();

        b.add::<PullServiceImpl>()
            .bind::<dyn PullService, PullServiceImpl>();

        b.add_value(workspace_layout.clone())
            .add_value(ContainerRuntime::default())
            .add_value(utils::ipfs_wrapper::IpfsClient::default())
            .add_value(IpfsGateway::default());

        let catalog = b.build();

        let pull_service = catalog.get_one::<dyn PullService>().unwrap();

        Self {
            tempdir,
            catalog,
            pull_service,
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
                &mut vec![dataset_ref].into_iter(),
                PullOptions {
                    sync_options: SyncOptions {
                        create_if_not_exists: true,
                        ..SyncOptions::default()
                    },
                    ..PullOptions::default()
                },
                None,
                None,
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

    pub fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
