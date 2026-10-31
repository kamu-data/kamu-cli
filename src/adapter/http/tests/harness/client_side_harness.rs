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

use container_runtime::ContainerRuntime;
use database_common::NoOpDatabasePlugin;
use headers::Header;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::utils::url_ext::UrlExt;
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_inmem::{
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::*;
use kamu_adapter_http::smart_protocol::messages::{
    DatasetPushMetadataRequest,
    DatasetPushRequest,
    DatasetPushResponse,
    SMART_TRANSFER_PROTOCOL_VERSION,
};
use kamu_adapter_http::smart_protocol::protocol_dataset_helper::{
    prepare_dataset_metadata_batch,
    prepare_dataset_transfer_plan,
};
use kamu_adapter_http::{OdfSmtpVersion, SmartTransferProtocolClientWs};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacDatasetRegistryFacadeImpl,
    RebacServiceImpl,
};
use kamu_datasets::*;
use kamu_datasets_services::testing::RacingDatasetHeadCommitter;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use odf::dataset::{DatasetFactoryImpl, DatasetLayout, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;

use super::{
    RawProtocolSocket,
    RawPullSession,
    RawPushSession,
    append_unreferenced_block,
    commit_add_data_event,
    create_headless_dataset,
    find_stored_head,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CLIENT_ACCOUNT_NAME: &str = "kamu-client";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ClientSideHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    pull_dataset_use_case: Arc<dyn PullDatasetUseCase>,
    push_dataset_use_case: Arc<dyn PushDatasetUseCase>,
    access_token_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
    racing_committer: Arc<RacingDatasetHeadCommitter>,
    options: ClientSideHarnessOptions,
}

pub(crate) struct ClientSideHarnessOptions {
    pub tenancy_config: TenancyConfig,
    pub authenticated_remotely: bool,
}

impl ClientSideHarness {
    pub async fn new(options: ClientSideHarnessOptions) -> Self {
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

        b.add_builder(messaging_outbox::OutboxImmediateImpl::builder(
            messaging_outbox::ConsumerFilter::AllConsumers,
        ))
        .bind::<dyn Outbox, OutboxImmediateImpl>();

        b.add::<DependencyGraphServiceImpl>();
        kamu_datasets_inmem::register_dependencies(&mut b);

        match options.tenancy_config {
            TenancyConfig::SingleTenant => {
                b.add_value(CurrentAccountSubject::new_test());
                b.add_value(PredefinedAccountsConfig::test_single_tenant_with_id());
            }
            TenancyConfig::MultiTenant => {
                b.add_value(CurrentAccountSubject::logged(
                    kamu_accounts::Account::seed_resource_id_from_name(CLIENT_ACCOUNT_NAME),
                    odf::metadata::testing::account_id(&CLIENT_ACCOUNT_NAME),
                    odf::AccountName::new_unchecked(CLIENT_ACCOUNT_NAME),
                ));

                let mut predefined_accounts_config = PredefinedAccountsConfig::new();
                predefined_accounts_config.predefined.push(
                    AccountConfig::test_config_from_name_with_id(odf::AccountName::new_unchecked(
                        CLIENT_ACCOUNT_NAME,
                    )),
                );
                b.add_value(predefined_accounts_config);
            }
        }
        b.add_value(AuthConfig::sample());

        b.add::<AlwaysHappyDatasetPermissionChecker>();

        if options.authenticated_remotely {
            b.add::<odf::dataset::DummyOdfServerAccessTokenResolver>();
        } else {
            b.add_value(odf::dataset::MockOdfServerAccessTokenResolver::empty());
            b.bind::<dyn odf::dataset::OdfServerAccessTokenResolver, odf::dataset::MockOdfServerAccessTokenResolver>();
        }

        b.add::<SystemTimeSourceDefault>();

        b.add_value(options.tenancy_config);

        b.add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
            datasets_dir,
        ));
        b.add::<DatasetLfsBuilderDatabaseBackedImpl>();
        b.add_value(MetadataChainDbBackedConfig::default());

        b.add::<RemoteRepositoryRegistryImpl>();

        b.add::<RemoteAliasesRegistryImpl>();
        b.add::<RemoteAliasResolverImpl>();
        b.add::<RemoteDatasetUrlResolverImpl>();

        b.add::<EngineProvisionerNull>();

        b.add::<ObjectStoreRegistryImpl>();
        b.add::<ObjectStoreBuilderLocalFs>();

        b.add::<DataFormatRegistryImpl>();

        b.add::<FetchService>();

        b.add_value(EngineConfigDatafusionEmbeddedIngest::default());
        b.add::<PollingIngestPlannerImpl>();
        b.add::<PollingIngestServiceImpl>();

        b.add::<DatasetFactoryImpl>();

        b.add::<SmartTransferProtocolClientWs>();
        b.add::<SimpleTransferProtocol>();

        b.add::<SyncServiceImpl>();

        b.add::<IpfsServiceImpl>();
        b.add::<LocalSyncCommitterImpl>();
        b.add::<SyncRequestBuilder>();

        b.add::<TransformPlannerImpl>();
        b.add::<TransformExecutorImpl>();

        b.add_value(EngineConfigDatafusionEmbeddedCompaction::default());
        b.add::<CompactionPlannerImpl>();
        b.add::<CompactionExecutorImpl>();

        b.add::<PullRequestPlannerImpl>();
        b.add::<DatasetUpdateKindResolverImpl>();
        b.add::<DatasetPullSourceResolverImpl>();

        b.add::<PushRequestPlannerImpl>();

        b.add::<DatasetKeyValueServiceSysEnv>();

        b.add::<AppendDatasetMetadataBatchUseCaseImpl>();
        b.add::<CommitDatasetEventUseCaseImpl>();
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
        b.add::<CreateDatasetUseCaseImpl>();
        b.add::<PullDatasetUseCaseImpl>();
        b.add::<DatasetOperationGuardImpl>();
        b.add::<DatasetUpdateStepsImpl>();
        b.add::<PushDatasetUseCaseImpl>();

        b.add::<DatasetReferenceServiceImpl>();
        b.add_value(RacingDatasetHeadCommitter::default())
            .bind::<dyn DatasetHeadCommitter, RacingDatasetHeadCommitter>();
        b.add::<CreateDatasetUseCaseHelper>();
        b.add::<DatasetEntryServiceImpl>();

        b.add::<InMemoryDidSecretKeyRepository>();

        b.add::<AccountServiceImpl>();
        b.add::<CreateAccountUseCaseImpl>();
        b.add::<UpdateAccountUseCaseImpl>();
        b.add::<ModifyAccountPasswordUseCaseImpl>();
        b.add::<InMemoryAccountRepository>();
        b.add::<LoginPasswordAuthProvider>();
        b.add::<PredefinedAccountsRegistrator>();
        b.add::<RebacServiceImpl>();
        b.add::<InMemoryRebacRepository>();
        b.add_value(DefaultAccountProperties::default());
        b.add_value(DefaultDatasetProperties::default());
        b.add_value(DidSecretEncryptionConfig::sample());

        b.add::<RebacDatasetRegistryFacadeImpl>();

        b.add_value(ContainerRuntime::default());
        b.add_value(kamu::utils::ipfs_wrapper::IpfsClient::default());
        b.add_value(IpfsGateway::default());

        b.add::<OAuthDeviceCodeServiceImpl>();
        b.add::<OAuthDeviceCodeGeneratorDefault>();
        b.add::<InMemoryOAuthDeviceCodeRepository>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            tempdir,
            pull_dataset_use_case: catalog.get_one().unwrap(),
            push_dataset_use_case: catalog.get_one().unwrap(),
            access_token_resolver: catalog.get_one().unwrap(),
            racing_committer: catalog.get_one().unwrap(),
            catalog,
            options,
        }
    }

    pub fn client_account_id(&self) -> odf::AccountID {
        let subject = self.catalog.get_one::<CurrentAccountSubject>().unwrap();
        subject.account_id().clone()
    }

    pub fn client_account_name(&self) -> odf::AccountName {
        let subject = self.catalog.get_one::<CurrentAccountSubject>().unwrap();
        subject.account_name().clone()
    }

    pub fn operating_account_name(&self) -> Option<odf::AccountName> {
        if self.options.tenancy_config == TenancyConfig::MultiTenant
            && self.options.authenticated_remotely
        {
            Some(odf::AccountName::new_unchecked(CLIENT_ACCOUNT_NAME))
        } else {
            None
        }
    }

    pub fn dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        self.catalog.get_one().unwrap()
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
        self.catalog.get_one().unwrap()
    }

    pub fn compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        self.catalog.get_one().unwrap()
    }

    pub fn dataset_entry_writer(&self) -> Arc<dyn DatasetEntryWriter> {
        self.catalog.get_one().unwrap()
    }

    pub fn dataset_reference_service(&self) -> Arc<dyn DatasetReferenceService> {
        self.catalog.get_one().unwrap()
    }

    /// Arms the next `HEAD` commit to first move `HEAD` to `concurrent_head`,
    /// the way a concurrent writer would
    pub fn move_head_before_next_commit(&self, concurrent_head: odf::Multihash) {
        self.racing_committer
            .move_head_before_next_commit(&self.catalog, concurrent_head);
    }

    pub async fn append_unreferenced_block(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> odf::Multihash {
        append_unreferenced_block(self.dataset_registry().as_ref(), dataset_handle).await
    }

    pub async fn stored_head(&self, dataset_id: &odf::DatasetID) -> odf::Multihash {
        self.dataset_reference_service()
            .get_reference(dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    }

    /// `None` while the dataset has no `HEAD`
    pub async fn find_stored_head(&self, dataset_id: &odf::DatasetID) -> Option<odf::Multihash> {
        find_stored_head(self.dataset_reference_service().as_ref(), dataset_id).await
    }

    /// A dataset of the operating account with its own `Seed` but no `HEAD`
    pub async fn create_headless_dataset(&self, dataset_name: &str) -> odf::DatasetHandle {
        create_headless_dataset(
            self.catalog
                .get_one::<CreateDatasetUseCaseHelper>()
                .unwrap()
                .as_ref(),
            &self.client_account_id(),
            &odf::DatasetAlias::new(
                self.operating_account_name(),
                odf::DatasetName::new_unchecked(dataset_name),
            ),
        )
        .await
    }

    // TODO: accept alias or handle
    pub fn dataset_layout(&self, dataset_id: &odf::DatasetID) -> DatasetLayout {
        let root_path = self
            .internal_datasets_folder_path()
            .join(dataset_id.as_multibase().to_stack_string());
        DatasetLayout::new(root_path.as_path())
    }

    pub async fn pull_datasets(
        &self,
        dataset_ref: odf::DatasetRefAny,
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

    pub async fn pull_dataset_result(
        &self,
        dataset_ref: odf::DatasetRefAny,
        force: bool,
    ) -> PullResult {
        self.pull_datasets(dataset_ref, force)
            .await
            .first()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .clone()
    }

    /// Pulls into an existing dataset through the update steps the pull use
    /// case runs, returning the sync result with the increment the use case
    /// does not report
    pub async fn sync_into_existing(
        &self,
        src_ref: odf::DatasetRefRemote,
        dataset_handle: &odf::DatasetHandle,
    ) -> SyncResult {
        let update_steps = self.catalog.get_one::<dyn DatasetUpdateSteps>().unwrap();
        let dst = update_steps
            .resolve_sync_target(&PullLocalTarget::existing(dataset_handle.clone()))
            .await
            .unwrap()
            .unwrap();

        update_steps
            .transfer_and_commit_sync(
                LocalSyncRequest { src: src_ref, dst },
                SyncOptions::default(),
                None,
                None,
            )
            .await
            .unwrap()
    }

    pub async fn push_dataset(
        &self,
        dataset_local_ref: odf::DatasetRef,
        dataset_remote_ref: odf::DatasetPushTarget,
        force: bool,
        dataset_visibility: odf::DatasetVisibility,
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
        dataset_local_ref: odf::DatasetRef,
        dataset_remote_ref: odf::DatasetPushTarget,
        force: bool,
        dataset_visibility: odf::DatasetVisibility,
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

    /// Creates a root dataset of the operating account with a schema and one
    /// data slice, and returns its local reference
    pub async fn create_root_dataset_with_data(&self, dataset_name: &str) -> odf::DatasetRef {
        let alias = odf::DatasetAlias::new(
            self.operating_account_name(),
            odf::DatasetName::new_unchecked(dataset_name),
        );
        let created = self
            .create_dataset_from_snapshot()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(alias.clone())
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();
        commit_add_data_event(
            self.dataset_registry().as_ref(),
            &created.dataset_handle,
            None,
        )
        .await;
        alias.as_local_ref()
    }

    /// Commits a metadata-only block, which references no objects
    pub async fn commit_set_info(&self, dataset_handle: &odf::DatasetHandle) -> odf::Multihash {
        self.commit_dataset_event()
            .execute(
                dataset_handle,
                MetadataFactory::set_info()
                    .description("pushed")
                    .build()
                    .into(),
            )
            .await
            .unwrap()
            .new_head
    }

    /// The push request and metadata for the blocks after `base_head`, as the
    /// client would send them
    pub async fn raw_push_requests(
        &self,
        dataset_handle: &odf::DatasetHandle,
        base_head: &odf::Multihash,
    ) -> (DatasetPushRequest, DatasetPushMetadataRequest) {
        let resolved_dataset = self
            .dataset_registry()
            .get_dataset_by_handle(dataset_handle)
            .await;
        let chain = resolved_dataset.as_metadata_chain();
        let head = chain.resolve_ref(&odf::BlockRef::Head).await.unwrap();

        let transfer_plan = prepare_dataset_transfer_plan(chain, &head, Some(base_head), false)
            .await
            .unwrap();
        let new_blocks = prepare_dataset_metadata_batch(chain, &head, Some(base_head), false)
            .await
            .unwrap();

        (
            DatasetPushRequest {
                current_head: Some(base_head.clone()),
                transfer_plan,
                force_update_if_diverged: false,
                visibility_for_created_dataset: odf::DatasetVisibility::Private,
            },
            DatasetPushMetadataRequest { new_blocks },
        )
    }

    pub async fn open_raw_push_session(&self, url: &Url) -> RawPushSession {
        RawPushSession::new(self.open_raw_protocol_socket(url, "push").await)
    }

    pub async fn open_raw_pull_session(&self, url: &Url) -> RawPullSession {
        RawPullSession::new(self.open_raw_protocol_socket(url, "pull").await)
    }

    async fn open_raw_protocol_socket(&self, url: &Url, method: &str) -> RawProtocolSocket {
        let (socket, _) =
            connect_async(self.websocket_request(url, method, SMART_TRANSFER_PROTOCOL_VERSION))
                .await
                .unwrap();
        RawProtocolSocket::new(socket)
    }

    pub fn add_repository(
        &self,
        repo_name: &odf::RepoName,
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

    /// Connects with an unsupported protocol version, returning the server's
    /// refusal
    pub async fn try_connect_to_websocket(&self, url: &Url, method: &str) -> Result<(), String> {
        match connect_async(self.websocket_request(url, method, 0)).await {
            Ok(_) => Ok(()),
            Err(ref _err @ Error::Http(ref http_err)) => {
                Err(std::str::from_utf8(http_err.body().as_ref().unwrap())
                    .unwrap()
                    .to_owned())
            }
            Err(_err) => Err("dummy".to_string()),
        }
    }

    /// Sends a push request as it is, bypassing the client's own checks, and
    /// returns the server's reply, which must be a message
    pub async fn send_raw_push_request(
        &self,
        url: &Url,
        request: &DatasetPushRequest,
    ) -> DatasetPushResponse {
        let mut socket = self.open_raw_protocol_socket(url, "push").await;
        socket.send(request).await;
        socket.receive().await
    }

    fn websocket_request(
        &self,
        url: &Url,
        method: &str,
        protocol_version: i32,
    ) -> http::Request<()> {
        let mut ws_url = url.odf_to_transport_protocol().unwrap();
        ws_url.ensure_trailing_slash();
        let maybe_access_token = self
            .access_token_resolver
            .resolve_odf_dataset_access_token(&ws_url);

        ws_url = ws_url.join(method).unwrap();
        ws_url.set_scheme("ws").unwrap();
        let mut request = ws_url.into_client_request().unwrap();
        request.headers_mut().append(
            OdfSmtpVersion::name(),
            http::HeaderValue::from(protocol_version),
        );
        if let Some(access_token) = maybe_access_token {
            request.headers_mut().append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        }
        request
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
