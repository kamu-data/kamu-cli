// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
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
    CollectionEntry,
    CollectionEntryRecord,
    CollectionPath,
    CollectionUpdateOperation,
    DatasetRegistry,
    DatasetSnapshots,
    ExtraDataFields,
    MockDatasetActionAuthorizer,
    ResolvedDataset,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesSuccess,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    WriteCheckedDataset,
};
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    UpdateCollectionEntriesUseCaseImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::OperationType as Op;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Collection datasets populated through [`UpdateCollectionEntriesUseCase`], with
/// direct access to their changelog and projected state for assertions
#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
pub struct CollectionBaseUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    catalog: dill::Catalog,
    update_use_case: Arc<dyn UpdateCollectionEntriesUseCase>,
    push_ingest_use_case: Arc<dyn PushIngestDataUseCase>,
    query_svc: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl CollectionBaseUseCaseHarness {
    pub fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .without_outbox()
                .with_maybe_authorizer(Some(MockDatasetActionAuthorizer::new())),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        let catalog = b
            .add::<UpdateCollectionEntriesUseCaseImpl>()
            .add::<PushIngestDataUseCaseImpl>()
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
            .add::<InMemoryDatasetStatisticsRepository>()
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
            update_use_case: catalog.get_one().unwrap(),
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

    /// Datasets the entries link to; the use cases do not require them to exist
    pub fn linked_datasets(&self) -> [odf::DatasetID; 3] {
        [b"foo", b"bar", b"baz"].map(|seed| odf::DatasetID::new_seeded_ed25519(seed))
    }

    pub fn add(&self, path: &str, reference: &odf::DatasetID) -> CollectionUpdateOperation {
        self.add_with(path, reference, serde_json::json!({}))
    }

    pub fn add_with(
        &self,
        path: &str,
        reference: &odf::DatasetID,
        extra_data: serde_json::Value,
    ) -> CollectionUpdateOperation {
        CollectionUpdateOperation::add(
            CollectionPath::new(path.to_string()),
            reference.clone(),
            Self::extra_data(extra_data),
        )
    }

    pub fn mv(&self, path_from: &str, path_to: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::r#move(
            CollectionPath::new(path_from.to_string()),
            CollectionPath::new(path_to.to_string()),
            None,
        )
    }

    pub fn mv_with(
        &self,
        path_from: &str,
        path_to: &str,
        extra_data: serde_json::Value,
    ) -> CollectionUpdateOperation {
        CollectionUpdateOperation::r#move(
            CollectionPath::new(path_from.to_string()),
            CollectionPath::new(path_to.to_string()),
            Some(Self::extra_data(extra_data)),
        )
    }

    pub fn remove(&self, path: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::remove(CollectionPath::new(path.to_string()))
    }

    pub fn record(&self, path: &str, reference: &odf::DatasetID) -> CollectionEntryRecord {
        self.record_with(path, reference, serde_json::json!({}))
    }

    pub fn record_with(
        &self,
        path: &str,
        reference: &odf::DatasetID,
        extra_data: serde_json::Value,
    ) -> CollectionEntryRecord {
        CollectionEntryRecord {
            path: CollectionPath::new(path.to_string()),
            reference: reference.clone(),
            extra_data: Self::extra_data(extra_data),
        }
    }

    fn extra_data(value: serde_json::Value) -> ExtraDataFields {
        let serde_json::Value::Object(map) = value else {
            panic!("Extra data must be a JSON object, got: {value}");
        };
        ExtraDataFields::new(map)
    }

    pub async fn create_collection(
        &self,
        extra_columns: Vec<odf::schema::DataField>,
    ) -> ResolvedDataset {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("collection"));
        let snapshot =
            DatasetSnapshots::collection(alias.clone(), extra_columns, Vec::new()).unwrap();

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

    /// Writes changelog records directly to the push source of the collection, as any
    /// client with write access can, bypassing the update use case
    pub async fn push_raw_records(
        &self,
        collection: &ResolvedDataset,
        records: Vec<serde_json::Value>,
    ) {
        let ndjson = records
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");

        self.push_ingest_use_case
            .execute(
                collection.clone(),
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

    pub async fn update(
        &self,
        collection: &ResolvedDataset,
        source_event_time: Option<DateTime<Utc>>,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError> {
        self.update_use_case
            .execute(
                WriteCheckedDataset::from_ref(collection),
                source_event_time,
                operations,
                expected_head,
            )
            .await
    }

    pub async fn update_success(
        &self,
        collection: &ResolvedDataset,
        operations: Vec<CollectionUpdateOperation>,
    ) -> UpdateCollectionEntriesSuccess {
        self.update_success_at(collection, None, operations).await
    }

    pub async fn update_success_at(
        &self,
        collection: &ResolvedDataset,
        source_event_time: Option<DateTime<Utc>>,
        operations: Vec<CollectionUpdateOperation>,
    ) -> UpdateCollectionEntriesSuccess {
        let res = self
            .update(collection, source_event_time, operations, None)
            .await
            .unwrap();
        let UpdateCollectionEntriesResult::Success(success) = res else {
            panic!("Expected a successful update, got: {res:?}");
        };
        success
    }

    pub async fn chain_len(&self, collection: &ResolvedDataset) -> u64 {
        use odf::dataset::MetadataChainExt;

        let head = collection
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await
            .unwrap();
        head.sequence_number + 1
    }

    /// Raw changelog records as `(offset, op, path, ref)`, ordered by offset
    pub async fn ledger(
        &self,
        collection: &ResolvedDataset,
    ) -> Vec<(u64, Op, String, odf::DatasetID)> {
        let mut ledger: Vec<_> = self
            .collect_records(collection, false)
            .await
            .into_iter()
            .map(|record| {
                (
                    record["offset"].as_u64().unwrap(),
                    Op::try_from(u8::try_from(record["op"].as_u64().unwrap()).unwrap()).unwrap(),
                    record["path"].as_str().unwrap().to_string(),
                    odf::DatasetID::from_did_str(record["ref"].as_str().unwrap()).unwrap(),
                )
            })
            .collect();

        ledger.sort_by_key(|(offset, ..)| *offset);
        ledger
    }

    /// Current entries projected from the whole changelog, ordered by path
    pub async fn entries(&self, collection: &ResolvedDataset) -> Vec<CollectionEntryRecord> {
        let mut entries: Vec<CollectionEntryRecord> = self
            .collect_records(collection, true)
            .await
            .into_iter()
            .map(|record| CollectionEntry::from_json(record).unwrap().into())
            .collect();

        entries.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        entries
    }

    async fn collect_records(
        &self,
        collection: &ResolvedDataset,
        project: bool,
    ) -> Vec<serde_json::Value> {
        let df = self
            .query_svc
            .get_data(collection.clone(), GetDataOptions::default())
            .await
            .unwrap()
            .df
            .unwrap();

        let df = if project {
            odf::utils::data::changelog::project(
                df,
                &["path".to_string()],
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
