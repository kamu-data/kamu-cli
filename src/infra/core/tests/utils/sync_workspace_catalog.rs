// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use kamu::domain::*;
use kamu::testing::DummySmartTransferProtocolClient;
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::local_sync_writes::PulledSeedStore;
use kamu::utils::simple_transfer_protocol::{SimpleTransferProtocol, SimpleTransferProtocolConfig};
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, CurrentAccountSubjectCatalogExt as _};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    DeleteDatasetRebacPropertiesUseCaseImpl,
    RebacDatasetLifecycleMessageConsumer,
    RebacServiceImpl,
    SetDatasetRebacPropertiesUseCaseImpl,
};
use kamu_datasets::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::*;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The catalog of a single-tenant workspace that syncs datasets with
/// local-file and simple-protocol remotes. Smart transfers are not wired.
/// Access properties of created datasets are recorded in memory
pub struct SyncWorkspaceCatalog {
    workspace_dir: PathBuf,
    ipfs_gateway: odf::dataset::IpfsGateway,
    ipfs_client: IpfsClient,
    dataset_entry_writer: MockDatasetEntryWriter,
    maybe_simple_transfer_config: Option<SimpleTransferProtocolConfig>,
}

impl SyncWorkspaceCatalog {
    /// Datasets live under `<workspace_dir>/datasets`, repositories under
    /// `<workspace_dir>/repos`
    pub fn new(workspace_dir: &Path) -> Self {
        Self {
            workspace_dir: workspace_dir.to_path_buf(),
            ipfs_gateway: odf::dataset::IpfsGateway::default(),
            ipfs_client: IpfsClient::default(),
            dataset_entry_writer: MockDatasetEntryWriter::new(),
            maybe_simple_transfer_config: None,
        }
    }

    pub fn with_ipfs(mut self, gateway: odf::dataset::IpfsGateway, client: IpfsClient) -> Self {
        self.ipfs_gateway = gateway;
        self.ipfs_client = client;
        self
    }

    /// Expectations for creating dataset entries; none are expected by default
    pub fn with_dataset_entry_writer(mut self, writer: MockDatasetEntryWriter) -> Self {
        self.dataset_entry_writer = writer;
        self
    }

    /// Without it, the simple protocol reads its settings from the environment
    pub fn with_simple_transfer_config(mut self, config: SimpleTransferProtocolConfig) -> Self {
        self.maybe_simple_transfer_config = Some(config);
        self
    }

    pub fn build(self) -> dill::Catalog {
        let datasets_dir = self.workspace_dir.join("datasets");
        std::fs::create_dir_all(&datasets_dir).unwrap();

        let mut b = dill::CatalogBuilder::new();
        kamu_datasets_inmem::register_dependencies(&mut b);
        b.add::<DidGeneratorDefault>()
            .add::<SystemTimeSourceDefault>()
            .add_value(self.ipfs_gateway)
            .add_value(self.ipfs_client)
            .add_current_account_subject(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(RemoteReposDir::new_with_creation(
                self.workspace_dir.join("repos"),
            ))
            .add::<RemoteRepositoryRegistryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<odf::dataset::DatasetFactoryImpl>()
            .add::<SyncServiceImpl>()
            .add::<IpfsServiceImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add::<PulledSeedStore>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add_value(self.dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add::<RebacDatasetLifecycleMessageConsumer>()
            .add::<SetDatasetRebacPropertiesUseCaseImpl>()
            .add::<DeleteDatasetRebacPropertiesUseCaseImpl>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default());

        if let Some(config) = self.maybe_simple_transfer_config {
            b.add_value(config);
        }

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );
        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
