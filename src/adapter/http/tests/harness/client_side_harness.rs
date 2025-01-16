// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use auth::OdfServerAccessTokenResolver;
use container_runtime::ContainerRuntime;
use database_common::NoOpDatabasePlugin;
use dill::Component;
use headers::Header;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_adapter_http::{OdfSmtpVersion, SmartTransferProtocolClientWs};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{DatasetKeyValueServiceSysEnv, DependencyGraphServiceImpl};
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::{
    AccountID,
    AccountName,
    DatasetID,
    DatasetPushTarget,
    DatasetRef,
    DatasetRefAny,
    RepoName,
};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CLIENT_ACCOUNT_NAME: &str = "kamu-client";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ClientSideHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    pull_dataset_use_case: Arc<dyn PullDatasetUseCase>,
    push_dataset_use_case: Arc<dyn PushDatasetUseCase>,
    access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
    options: ClientSideHarnessOptions,
}

pub(crate) struct ClientSideHarnessOptions {
    pub tenancy_config: TenancyConfig,
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

        b.add::<DidGeneratorDefault>();

        b.add_value(RunInfoDir::new(run_info_dir));
        b.add_value(CacheDir::new(cache_dir));
        b.add_value(RemoteReposDir::new(repos_dir));

        b.add::<DummyOutboxImpl>();

        b.add::<DependencyGraphServiceImpl>();
        b.add::<InMemoryDatasetDependencyRepository>();

        b.add_value(CurrentAccountSubject::logged(
            AccountID::new_seeded_ed25519(CLIENT_ACCOUNT_NAME.as_bytes()),
            AccountName::new_unchecked(CLIENT_ACCOUNT_NAME),
            false,
        ));

        b.add::<auth::AlwaysHappyDatasetActionAuthorizer>();

        if options.authenticated_remotely {
            b.add::<auth::DummyOdfServerAccessTokenResolver>();
        } else {
            b.add_value(kamu::testing::MockOdfServerAccessTokenResolver::empty());
            b.bind::<dyn auth::OdfServerAccessTokenResolver, kamu::testing::MockOdfServerAccessTokenResolver>();
        }

        b.add::<SystemTimeSourceDefault>();

        b.add_value(options.tenancy_config);

        b.add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>();

        b.add::<RemoteRepositoryRegistryImpl>();

        b.add::<RemoteAliasesRegistryImpl>();
        b.add::<RemoteAliasResolverImpl>();

        b.add::<EngineProvisionerNull>();

        b.add::<ObjectStoreRegistryImpl>();
        b.add::<ObjectStoreBuilderLocalFs>();

        b.add::<DataFormatRegistryImpl>();

        b.add::<FetchService>();

        b.add::<PollingIngestServiceImpl>();

        b.add::<DatasetFactoryImpl>();

        b.add::<SmartTransferProtocolClientWs>();
        b.add::<SimpleTransferProtocol>();

        b.add::<SyncServiceImpl>();
        b.add::<SyncRequestBuilder>();

        b.add::<TransformRequestPlannerImpl>();
        b.add::<TransformElaborationServiceImpl>();
        b.add::<TransformExecutorImpl>();

        b.add::<CompactionPlannerImpl>();
        b.add::<CompactionExecutorImpl>();

        b.add::<PullRequestPlannerImpl>();

        b.add::<PushRequestPlannerImpl>();

        b.add::<DatasetKeyValueServiceSysEnv>();

        b.add::<AppendDatasetMetadataBatchUseCaseImpl>();
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
        b.add::<CommitDatasetEventUseCaseImpl>();
        b.add::<CreateDatasetUseCaseImpl>();
        b.add::<PullDatasetUseCaseImpl>();
        b.add::<PushDatasetUseCaseImpl>();

        b.add_value(ContainerRuntime::default());
        b.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());
        b.add_value(IpfsGateway::default());

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let pull_dataset_use_case = catalog.get_one::<dyn PullDatasetUseCase>().unwrap();
        let push_dataset_use_case = catalog.get_one::<dyn PushDatasetUseCase>().unwrap();
        let access_token_resolver = catalog
            .get_one::<dyn OdfServerAccessTokenResolver>()
            .unwrap();

        Self {
            tempdir,
            catalog,
            pull_dataset_use_case,
            push_dataset_use_case,
            access_token_resolver,
            options,
        }
    }

    pub fn operating_account_name(&self) -> Option<AccountName> {
        if self.options.tenancy_config == TenancyConfig::MultiTenant
            && self.options.authenticated_remotely
        {
            Some(AccountName::new_unchecked(CLIENT_ACCOUNT_NAME))
        } else {
            None
        }
    }

    pub fn dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        self.catalog.get_one::<dyn DatasetRegistry>().unwrap()
    }

    pub fn create_dataset_from_snapshot(&self) -> Arc<dyn CreateDatasetFromSnapshotUseCase> {
        self.catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap()
    }

    pub fn commit_dataset_event(&self) -> Arc<dyn CommitDatasetEventUseCase> {
        self.catalog
            .get_one::<dyn CommitDatasetEventUseCase>()
            .unwrap()
    }

    pub fn compaction_planner(&self) -> Arc<dyn CompactionPlanner> {
        self.catalog.get_one::<dyn CompactionPlanner>().unwrap()
    }

    pub fn compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        self.catalog.get_one::<dyn CompactionExecutor>().unwrap()
    }

    // TODO: accept alias or handle
    pub fn dataset_layout(&self, dataset_id: &DatasetID, dataset_name: &str) -> DatasetLayout {
        let root_path = match self.options.tenancy_config {
            TenancyConfig::MultiTenant => self
                .internal_datasets_folder_path()
                .join(CLIENT_ACCOUNT_NAME)
                .join(dataset_id.as_multibase().to_stack_string()),
            TenancyConfig::SingleTenant => self.internal_datasets_folder_path().join(dataset_name),
        };
        DatasetLayout::new(root_path.as_path())
    }

    pub async fn pull_datasets(
        &self,
        dataset_ref: DatasetRefAny,
        force: bool,
    ) -> Vec<PullResponse> {
        self.pull_dataset_use_case
            .execute_multi(
                vec![PullRequest::from_any_ref(&dataset_ref, |_| {
                    self.options.tenancy_config == TenancyConfig::SingleTenant
                })],
                PullOptions {
                    sync_options: SyncOptions {
                        create_if_not_exists: true,
                        force,
                        ..SyncOptions::default()
                    },
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
    }

    pub async fn pull_dataset_result(&self, dataset_ref: DatasetRefAny, force: bool) -> PullResult {
        self.pull_datasets(dataset_ref, force)
            .await
            .first()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .clone()
    }

    pub async fn push_dataset(
        &self,
        dataset_local_ref: DatasetRef,
        dataset_remote_ref: DatasetPushTarget,
        force: bool,
        dataset_visibility: DatasetVisibility,
    ) -> Vec<PushResponse> {
        let dataset_handle = self
            .dataset_registry()
            .resolve_dataset_handle_by_ref(&dataset_local_ref)
            .await
            .unwrap();

        self.push_dataset_use_case
            .execute_multi(
                vec![dataset_handle],
                PushMultiOptions {
                    sync_options: SyncOptions {
                        create_if_not_exists: true,
                        force,
                        dataset_visibility,
                        ..SyncOptions::default()
                    },
                    remote_target: Some(dataset_remote_ref),
                    ..PushMultiOptions::default()
                },
                None,
            )
            .await
            .unwrap()
    }

    pub async fn push_dataset_result(
        &self,
        dataset_local_ref: DatasetRef,
        dataset_remote_ref: DatasetPushTarget,
        force: bool,
        dataset_visibility: DatasetVisibility,
    ) -> SyncResult {
        let results = self
            .push_dataset(
                dataset_local_ref,
                dataset_remote_ref,
                force,
                dataset_visibility,
            )
            .await;

        match &(results.first().unwrap().result) {
            Ok(sync_result) => sync_result.clone(),
            Err(e) => {
                println!("Error: {e:#?}");
                panic!("Failure")
            }
        }
    }

    pub fn add_repository(
        &self,
        repo_name: &RepoName,
        base_url: &str,
    ) -> Result<(), InternalError> {
        let remote_repo_reg = self
            .catalog
            .get_one::<dyn RemoteRepositoryRegistry>()
            .unwrap();

        remote_repo_reg
            .add_repository(
                repo_name,
                Url::from_str(&format!("http://{base_url}")).unwrap(),
            )
            .int_err()
    }

    pub fn internal_datasets_folder_path(&self) -> PathBuf {
        self.tempdir.path().join("datasets")
    }

    pub async fn try_connect_to_websocket(&self, url: &Url, method: &str) -> Result<(), String> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut ws_url = url.odf_to_transport_protocol().unwrap();
        ws_url.ensure_trailing_slash();
        let maybe_access_token = self
            .access_token_resolver
            .resolve_odf_dataset_access_token(&ws_url);

        ws_url = ws_url.join(method).unwrap();
        ws_url.set_scheme("ws").unwrap();
        let mut request = ws_url.into_client_request().unwrap();
        request
            .headers_mut()
            .append(OdfSmtpVersion::name(), http::HeaderValue::from(0));
        if let Some(access_token) = maybe_access_token {
            request.headers_mut().append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        }

        match connect_async(request).await {
            Ok(_) => Ok(()),
            Err(ref _err @ Error::Http(ref http_err)) => {
                Err(std::str::from_utf8(http_err.body().as_ref().unwrap())
                    .unwrap()
                    .to_owned())
            }
            Err(_err) => Err("dummy".to_string()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
