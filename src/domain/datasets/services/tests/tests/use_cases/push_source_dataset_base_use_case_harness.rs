// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use file_utils::MediaType;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{
    DataFormatRegistryImpl,
    EngineConfigDatafusionEmbeddedBatchQuery,
    EngineConfigDatafusionEmbeddedIngest,
    EngineProvisionerNull,
    ObjectStoreBuilderLocalFs,
    ObjectStoreRegistryImpl,
    PushIngestDataUseCaseImpl,
    PushIngestExecutorImpl,
    PushIngestPlannerImpl,
    QueryServiceImpl,
    SessionContextBuilder,
};
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryAccountRepository};
use kamu_accounts_services::{AccountQuotaServiceImpl, AccountServiceImpl};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_core::{
    DataSource,
    DidGenerator,
    GetDataOptions,
    PushIngestDataUseCase,
    PushIngestDataUseCaseOptions,
    QueryService,
};
use kamu_datasets::{
    DatasetRegistry,
    ExtraDataFields,
    MockDatasetPermissionChecker,
    ResolvedDataset,
};
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetHeadCommitterImpl,
    DatasetStatisticsServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Datasets written through push ingest and read through the query service, as the
/// dataset archetypes (collections, versioned files) are
#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
pub struct PushSourceDatasetBaseUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl PushSourceDatasetBaseUseCaseHarness {
    pub fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .without_outbox()
                .with_maybe_permission_checker(Some(MockDatasetPermissionChecker::allowing())),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        kamu_datasets_inmem::register_dependencies(&mut b);

        let catalog = b
            .add::<PushIngestDataUseCaseImpl>()
            .add::<DatasetHeadCommitterImpl>()
            .add::<PushIngestExecutorImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DummyOutboxImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<EngineProvisionerNull>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryAccountQuotaEventStore>()
            .add::<AccountQuotaServiceImpl>()
            .add::<DatasetStatisticsServiceImpl>()
            .add_value(kamu_datasets_services::QuotaDefaultsConfig::default())
            .add::<AccountQuotaCheckerStorageImpl>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<QueryServiceImpl>()
            .add::<SessionContextBuilder>()
            .add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
            .build();

        Self {
            base_use_case_harness,
            push_ingest_use_case: catalog.get_one().unwrap(),
            query_svc: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn extra_data(value: serde_json::Value) -> ExtraDataFields {
        let serde_json::Value::Object(map) = value else {
            panic!("Extra data must be a JSON object, got: {value}");
        };
        ExtraDataFields::new(map)
    }

    pub async fn create_dataset(&self, snapshot: odf::DatasetSnapshot) -> ResolvedDataset {
        let alias = snapshot.name.clone();

        let store_result = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        ResolvedDataset::from_stored(&store_result, &alias)
    }

    /// Writes records directly to the push source of the dataset, as any client with
    /// write access can, bypassing the archetype's use cases
    pub async fn push_ndjson(&self, dataset: &ResolvedDataset, records: Vec<serde_json::Value>) {
        let ndjson = records
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");

        self.push_ingest_use_case
            .execute(
                dataset.clone(),
                DataSource::Buffer(bytes::Bytes::from(ndjson)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: Some(MediaType::NDJSON.to_owned()),
                    expected_head: None,
                    skip_quota_check: true,
                },
                None,
            )
            .await
            .unwrap();
    }

    pub async fn chain_len(&self, dataset: &ResolvedDataset) -> u64 {
        use odf::dataset::MetadataChainExt;

        let head = dataset
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await
            .unwrap();
        head.sequence_number + 1
    }

    /// All records of the dataset as JSON, either the raw changelog or its state
    /// projected by the specified primary key
    pub async fn records(
        &self,
        dataset: &ResolvedDataset,
        project_by: Option<&[&str]>,
    ) -> Vec<serde_json::Value> {
        let df = self
            .query_svc
            .get_data(dataset.clone(), GetDataOptions::default())
            .await
            .unwrap()
            .df
            .unwrap();

        let df = if let Some(primary_key) = project_by {
            odf::utils::data::changelog::project(
                df,
                &primary_key
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                &odf::metadata::DatasetVocabulary::default(),
            )
            .unwrap()
        } else {
            df
        };

        df.collect_json_aos().await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
