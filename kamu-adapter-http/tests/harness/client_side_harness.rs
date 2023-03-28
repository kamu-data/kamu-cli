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
    domain::{DatasetRepository, PullOptions, PullResponse, PullService, SyncOptions},
    infra::{
        self, DatasetFactoryImpl, DatasetRepositoryLocalFs, EngineProvisionerNull,
        IngestServiceImpl, IpfsGateway, PullServiceImpl, RemoteAliasesRegistryImpl,
        RemoteRepositoryRegistryImpl, SyncServiceImpl, TransformServiceImpl, WorkspaceLayout,
    },
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
        let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path()).unwrap());

        let catalog = dill::CatalogBuilder::new()
            .add_value(DatasetRepositoryLocalFs::new(workspace_layout.clone()))
            .bind::<dyn DatasetRepository, infra::DatasetRepositoryLocalFs>()
            .build();

        let client_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
        let remote_alias_reg = Arc::new(RemoteAliasesRegistryImpl::new(
            client_repo.clone(),
            workspace_layout.clone(),
        ));
        let ingest_svc = Arc::new(IngestServiceImpl::new(
            workspace_layout.clone(),
            client_repo.clone(),
            Arc::new(EngineProvisionerNull),
            Arc::new(ContainerRuntime::default()),
        ));
        let transform_svc = Arc::new(TransformServiceImpl::new(
            client_repo.clone(),
            Arc::new(EngineProvisionerNull),
            workspace_layout.clone(),
        ));
        let sync_svc = Arc::new(SyncServiceImpl::new(
            remote_repo_reg.clone(),
            client_repo.clone(),
            Arc::new(DatasetFactoryImpl::new()),
            Arc::new(WsSmartTransferProtocolClient {}),
            Arc::new(kamu::infra::utils::ipfs_wrapper::IpfsClient::default()),
            IpfsGateway::default(),
        ));
        let pull_service = Arc::new(PullServiceImpl::new(
            client_repo.clone(),
            remote_alias_reg.clone(),
            ingest_svc,
            transform_svc,
            sync_svc,
        ));

        Self {
            tempdir,
            catalog,
            pull_service,
        }
    }

    pub fn dataset_repository(&self) -> Arc<dyn DatasetRepository> {
        self.catalog.get_one::<dyn DatasetRepository>().unwrap()
    }

    pub async fn pull_dataset(&self, dataset_ref: DatasetRefAny) -> Vec<PullResponse> {
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

    pub fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
