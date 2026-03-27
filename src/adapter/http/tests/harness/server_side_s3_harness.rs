// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::*;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacDatasetRegistryFacadeImpl,
    RebacServiceImpl,
};
use kamu_core::{DidGeneratorDefault, TenancyConfig};
use kamu_datasets::*;
use kamu_datasets_inmem::{
    InMemoryDatasetDataBlockRepository,
    InMemoryDatasetDependencyRepository,
    InMemoryDatasetEntryRepository,
    InMemoryDatasetKeyBlockRepository,
    InMemoryDatasetReferenceRepository,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceStub};
use url::Url;

use super::{
    DatasetTransferScope,
    SERVER_ACCOUNT_NAME,
    ServerSideDatasetFixture,
    ServerSideHarness,
    ServerSideHarnessOptions,
    TestAPIServer,
    create_cli_user_catalog,
    create_web_user_catalog,
    make_server_account,
};
use crate::harness::PROTOCOL_TRANSFER_SUBDIRS;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub(crate) struct ServerSideS3Harness {
    s3: LocalS3Server,
    base_catalog: dill::Catalog,
    api_server: TestAPIServer,
    options: ServerSideHarnessOptions,
    time_source: SystemTimeSourceStub,
    _temp_dir: tempfile::TempDir,
    account: Account,
}

struct S3DatasetFixture {
    s3_context: s3_utils::S3Context,
}

impl S3DatasetFixture {
    fn server_dataset_prefix(dataset_handle: &odf::DatasetHandle) -> String {
        dataset_handle
            .id
            .as_multibase()
            .to_stack_string()
            .to_string()
    }

    async fn read_server_dataset_head(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<String, InternalError> {
        let head_key = format!("{}/refs/head", Self::server_dataset_prefix(dataset_handle));

        let head_object = self.s3_context.get_object(head_key).await.int_err()?;
        let head_bytes = head_object.body.collect().await.int_err()?.into_bytes();

        String::from_utf8(head_bytes.to_vec()).int_err()
    }

    async fn list_server_dir_files(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dir_name: &str,
    ) -> Result<Vec<PathBuf>, InternalError> {
        let dir_prefix = format!(
            "{}/{dir_name}/",
            Self::server_dataset_prefix(dataset_handle)
        );

        let mut files = self
            .s3_context
            .bucket_list_object_keys(&dir_prefix)
            .await?
            .into_iter()
            .map(|key| PathBuf::from(key.strip_prefix(&dir_prefix).unwrap()))
            .collect::<Vec<_>>();

        files.sort();
        Ok(files)
    }

    #[allow(clippy::assigning_clones)]
    fn list_local_dir_files(dir: &Path) -> Vec<PathBuf> {
        if !dir.exists() {
            return Vec::new();
        }

        fn list_files_rec(dir: &Path) -> Vec<PathBuf> {
            std::fs::read_dir(dir)
                .unwrap()
                .flat_map(|entry| {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        list_files_rec(&path)
                    } else {
                        vec![path]
                    }
                })
                .collect()
        }

        let mut files = list_files_rec(dir);

        for path in &mut files {
            *path = path.strip_prefix(dir).unwrap().to_owned();
        }

        files.sort();
        files
    }
    fn dataset_root_relative_path(
        dataset_layout: &odf::dataset::DatasetLayout,
        path: &Path,
    ) -> PathBuf {
        path.strip_prefix(&dataset_layout.root_dir)
            .unwrap()
            .to_owned()
    }

    fn list_local_files_for_prefix(
        dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Vec<(PathBuf, PathBuf)> {
        fn list_files_rec(path: &Path) -> Vec<PathBuf> {
            std::fs::read_dir(path)
                .unwrap()
                .flat_map(|entry| {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        list_files_rec(&path)
                    } else {
                        vec![path]
                    }
                })
                .collect()
        }

        let files = match scope {
            DatasetTransferScope::Full => PROTOCOL_TRANSFER_SUBDIRS
                .into_iter()
                .map(|subdir| dataset_layout.root_dir.join(subdir))
                .filter(|path| path.exists())
                .flat_map(|path| list_files_rec(&path))
                .collect::<Vec<_>>(),
            DatasetTransferScope::DataOnly => {
                if !dataset_layout.data_dir.exists() {
                    Vec::new()
                } else {
                    list_files_rec(&dataset_layout.data_dir)
                }
            }
        };

        files
            .into_iter()
            .map(|path| {
                let rel_path = Self::dataset_root_relative_path(dataset_layout, &path);
                (path, rel_path)
            })
            .collect()
    }

    async fn list_server_keys_for_prefix(
        &self,
        dataset_handle: &odf::DatasetHandle,
        scope: DatasetTransferScope,
    ) -> Result<Vec<String>, InternalError> {
        match scope {
            DatasetTransferScope::Full => {
                let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
                let mut keys = Vec::new();
                for dir_name in PROTOCOL_TRANSFER_SUBDIRS {
                    let dir_prefix = format!("{dataset_prefix}/{dir_name}");
                    let mut dir_keys = self.s3_context.bucket_list_object_keys(&dir_prefix).await?;
                    keys.append(&mut dir_keys);
                }
                Ok(keys)
            }
            DatasetTransferScope::DataOnly => {
                let exact_prefix = format!("{}/data", Self::server_dataset_prefix(dataset_handle));
                let subtree_prefix = format!("{exact_prefix}/");
                let keys = self
                    .s3_context
                    .bucket_list_object_keys(&exact_prefix)
                    .await?;
                Ok(keys
                    .into_iter()
                    .filter(|key| key == &exact_prefix || key.starts_with(&subtree_prefix))
                    .collect())
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ServerSideDatasetFixture for S3DatasetFixture {
    async fn assert_dataset_in_sync(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
    ) -> Result<(), InternalError> {
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "blocks").await?,
            Self::list_local_dir_files(&client_dataset_layout.blocks_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "refs").await?,
            Self::list_local_dir_files(&client_dataset_layout.refs_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "data").await?,
            Self::list_local_dir_files(&client_dataset_layout.data_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "checkpoints")
                .await?,
            Self::list_local_dir_files(&client_dataset_layout.checkpoints_dir)
        );
        assert_eq!(
            self.read_server_dataset_head(dataset_handle).await?,
            std::fs::read_to_string(client_dataset_layout.refs_dir.join("head")).unwrap()
        );

        Ok(())
    }
    async fn download_dataset_to(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
        let server_keys = self
            .list_server_keys_for_prefix(dataset_handle, scope)
            .await?;

        for key in server_keys {
            let rel_path = key.strip_prefix(&format!("{dataset_prefix}/")).unwrap();
            let dst_path = client_dataset_layout.root_dir.join(rel_path);

            if let Some(parent_dir) = dst_path.parent() {
                std::fs::create_dir_all(parent_dir).int_err()?;
            }

            let object = self.s3_context.get_object(key).await.int_err()?;
            let data = object.body.collect().await.int_err()?.into_bytes();
            std::fs::write(dst_path, data).int_err()?;
        }

        Ok(())
    }

    async fn upload_dataset_from(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
        let local_files = Self::list_local_files_for_prefix(client_dataset_layout, scope);

        for (local_path, rel_path) in local_files {
            let key = format!("{dataset_prefix}/{}", rel_path.to_string_lossy());
            let data = std::fs::read(local_path).int_err()?;
            self.s3_context.put_object(key, &data).await.int_err()?;
        }

        Ok(())
    }

    async fn write_dataset_alias(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dataset_alias: &odf::DatasetAlias,
    ) -> Result<(), InternalError> {
        let key = format!("{}/info/alias", Self::server_dataset_prefix(dataset_handle));
        self.s3_context
            .put_object(key, dataset_alias.to_string().as_bytes())
            .await
            .int_err()?;
        Ok(())
    }
}

impl ServerSideS3Harness {
    pub async fn new(options: ServerSideHarnessOptions) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let s3 = LocalS3Server::new().await;
        s3.set_credentials_env_vars();

        let time_source = SystemTimeSourceStub::new();

        let account = make_server_account(options.tenancy_config);

        let predefined_accounts_config = match options.tenancy_config {
            TenancyConfig::SingleTenant => PredefinedAccountsConfig::single_tenant(),
            TenancyConfig::MultiTenant => {
                let mut predefined_accounts_config = PredefinedAccountsConfig::new();
                predefined_accounts_config
                    .predefined
                    .push(AccountConfig::test_config_from_name(
                        odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
                    ));
                predefined_accounts_config
            }
        };

        let jwt_authentication_config = JwtAuthenticationConfig {
            jwt_secret: "dummy".to_string(),
            maybe_dummy_token_account: Some(account.clone()),
        };

        let (base_catalog, listener) = {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            let base_url_rest = format!("http://{}", listener.local_addr().unwrap());

            let mut b = dill::CatalogBuilder::new();

            b.add_value(time_source.clone())
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
                .add::<DidGeneratorDefault>()
                .add_value(RunInfoDir::new(run_info_dir))
                .add_value(AuthConfig::sample())
                .add_builder(messaging_outbox::OutboxImmediateImpl::builder(
                    messaging_outbox::ConsumerFilter::AllConsumers,
                ))
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add_value(options.tenancy_config)
                .add_builder(odf::dataset::DatasetStorageUnitS3::builder(s3.ctx.clone()))
                .add::<kamu_datasets_services::DatasetS3BuilderDatabaseBackedImpl>()
                .add_value(kamu_datasets_services::MetadataChainDbBackedConfig::default())
                .add_value(ServerUrlConfig::new_test(Some(&base_url_rest)))
                .add_value(EngineConfigDatafusionEmbeddedCompaction::default())
                .add::<CompactionPlannerImpl>()
                .add::<CompactionExecutorImpl>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add_builder(ObjectStoreBuilderS3::builder(
                    s3.ctx.clone(),
                    true,
                    Some(s3.access_key.clone()),
                    Some(s3.secret_key.clone()),
                ))
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CommitDatasetEventUseCaseImpl>()
                .add::<CreateDatasetUseCaseHelper>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<InMemoryDatasetReferenceRepository>()
                .add::<InMemoryDatasetKeyBlockRepository>()
                .add::<InMemoryDatasetDataBlockRepository>()
                .add::<AuthenticationServiceImpl>()
                .add::<AccountServiceImpl>()
                .add::<CreateAccountUseCaseImpl>()
                .add::<UpdateAccountUseCaseImpl>()
                .add::<ModifyAccountPasswordUseCaseImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add_value(jwt_authentication_config)
                .add::<LoginPasswordAuthProvider>()
                .add::<PredefinedAccountsRegistrator>()
                .add::<RebacServiceImpl>()
                .add::<InMemoryRebacRepository>()
                .add_value(DefaultAccountProperties::default())
                .add_value(DefaultDatasetProperties::default())
                .add::<RebacDatasetRegistryFacadeImpl>()
                .add_value(predefined_accounts_config)
                .add::<OAuthDeviceCodeServiceImpl>()
                .add::<OAuthDeviceCodeGeneratorDefault>()
                .add::<InMemoryOAuthDeviceCodeRepository>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            register_message_dispatcher::<DatasetReferenceMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            );

            (b.build(), listener)
        };

        init_on_startup::run_startup_jobs(&base_catalog)
            .await
            .unwrap();

        let api_server = TestAPIServer::new(
            create_web_user_catalog(&base_catalog, &options),
            listener,
            options.tenancy_config,
        );

        Self {
            s3,
            base_catalog,
            api_server,
            options,
            time_source,
            _temp_dir: temp_dir,
            account,
        }
    }

    pub fn bucket(&self) -> &str {
        &self.s3.bucket
    }
}

#[async_trait::async_trait(?Send)]
impl ServerSideHarness for ServerSideS3Harness {
    fn server_account_id(&self) -> odf::AccountID {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => {
                odf::AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes())
            }
            TenancyConfig::SingleTenant => DEFAULT_ACCOUNT_ID.clone(),
        }
    }

    fn server_account_name(&self) -> odf::AccountName {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            TenancyConfig::SingleTenant => DEFAULT_ACCOUNT_NAME.clone(),
        }
    }

    fn operating_account_name(&self) -> Option<odf::AccountName> {
        match self.options.tenancy_config {
            TenancyConfig::MultiTenant => {
                Some(odf::AccountName::new_unchecked(SERVER_ACCOUNT_NAME))
            }
            TenancyConfig::SingleTenant => None,
        }
    }

    fn cli_dataset_registry(&self) -> Arc<dyn DatasetRegistry> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_create_dataset_use_case(&self) -> Arc<dyn CreateDatasetUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_create_dataset_from_snapshot_use_case(
        &self,
    ) -> Arc<dyn CreateDatasetFromSnapshotUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_commit_dataset_event_use_case(&self) -> Arc<dyn CommitDatasetEventUseCase> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_compaction_planner(&self) -> Arc<dyn CompactionPlanner> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_compaction_executor(&self) -> Arc<dyn CompactionExecutor> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_dataset_entry_writer(&self) -> Arc<dyn DatasetEntryWriter> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn cli_dataset_reference_service(&self) -> Arc<dyn DatasetReferenceService> {
        let cli_catalog = create_cli_user_catalog(&self.base_catalog, self.options.tenancy_config);
        cli_catalog.get_one().unwrap()
    }

    fn dataset_url_with_scheme(&self, dataset_alias: &odf::DatasetAlias, scheme: &str) -> Url {
        let api_server_address = self.api_server_addr();
        Url::from_str(
            match self.options.tenancy_config {
                TenancyConfig::MultiTenant => format!(
                    "{}://{}/{}/{}",
                    scheme,
                    api_server_address,
                    dataset_alias.account_name.as_ref().unwrap(),
                    dataset_alias.dataset_name
                ),
                TenancyConfig::SingleTenant => format!(
                    "{}://{}/{}",
                    scheme, api_server_address, dataset_alias.dataset_name
                ),
            }
            .as_str(),
        )
        .unwrap()
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn api_server_account(&self) -> Account {
        self.account.clone()
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub {
        &self.time_source
    }

    fn dataset_fixture(&self) -> Arc<dyn ServerSideDatasetFixture> {
        Arc::new(S3DatasetFixture {
            s3_context: self.s3.ctx.clone(),
        })
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
