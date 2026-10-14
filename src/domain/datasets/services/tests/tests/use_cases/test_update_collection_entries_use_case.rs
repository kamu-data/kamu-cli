// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

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
use kamu_core::{DidGenerator, GetDataOptions, QueryService};
use kamu_datasets::{
    CollectionEntryRecord,
    CollectionPath,
    CollectionUpdateOperation,
    DatasetRegistry,
    DatasetSnapshots,
    ExtraDataFields,
    MockDatasetActionAuthorizer,
    ReadCheckedDataset,
    ResolvedDataset,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesSuccess,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    ViewCollectionEntriesUseCase,
    WriteCheckedDataset,
};
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    AccountQuotaCheckerStorageImpl,
    DatasetStatisticsServiceImpl,
    UpdateCollectionEntriesUseCaseImpl,
    ViewCollectionEntriesUseCaseImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::OperationType as Op;
use pretty_assertions::assert_eq;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_commits_diff_in_one_block_in_order() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, bar, baz] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;
    let initial_chain_len = harness.chain_len(&collection).await;

    let res = harness
        .update_success(
            &collection,
            vec![
                UpdateCollectionEntriesUseCaseHarness::add("/a", &foo),
                UpdateCollectionEntriesUseCaseHarness::add("/b", &bar),
                UpdateCollectionEntriesUseCaseHarness::add("/c", &baz),
            ],
        )
        .await;

    assert_eq!(
        res.inserted_records,
        vec![
            (
                Op::Append,
                UpdateCollectionEntriesUseCaseHarness::record("/a", &foo)
            ),
            (
                Op::Append,
                UpdateCollectionEntriesUseCaseHarness::record("/b", &bar)
            ),
            (
                Op::Append,
                UpdateCollectionEntriesUseCaseHarness::record("/c", &baz)
            ),
        ]
    );
    assert_eq!(harness.chain_len(&collection).await, initial_chain_len + 1);

    // Re-adding `/a` retracts the old entry before appending the new one, moving `/c`
    // onto `/b` retracts both paths before appending `/b`
    let res = harness
        .update_success(
            &collection,
            vec![
                UpdateCollectionEntriesUseCaseHarness::add("/a", &bar),
                UpdateCollectionEntriesUseCaseHarness::mv("/c", "/b"),
            ],
        )
        .await;

    assert_eq!(
        res.inserted_records,
        vec![
            (
                Op::Retract,
                UpdateCollectionEntriesUseCaseHarness::record("/a", &foo)
            ),
            (
                Op::Append,
                UpdateCollectionEntriesUseCaseHarness::record("/a", &bar)
            ),
            (
                Op::Retract,
                UpdateCollectionEntriesUseCaseHarness::record("/b", &bar)
            ),
            (
                Op::Retract,
                UpdateCollectionEntriesUseCaseHarness::record("/c", &baz)
            ),
            (
                Op::Append,
                UpdateCollectionEntriesUseCaseHarness::record("/b", &baz)
            ),
        ]
    );
    assert_eq!(harness.chain_len(&collection).await, initial_chain_len + 2);

    assert_eq!(
        harness.ledger(&collection).await,
        vec![
            (0, Op::Append, "/a".to_string(), foo.clone()),
            (1, Op::Append, "/b".to_string(), bar.clone()),
            (2, Op::Append, "/c".to_string(), baz.clone()),
            (3, Op::Retract, "/a".to_string(), foo.clone()),
            (4, Op::Append, "/a".to_string(), bar.clone()),
            (5, Op::Retract, "/b".to_string(), bar.clone()),
            (6, Op::Retract, "/c".to_string(), baz.clone()),
            (7, Op::Append, "/b".to_string(), baz.clone()),
        ]
    );

    assert_eq!(
        harness.entries(&collection).await,
        vec![
            UpdateCollectionEntriesUseCaseHarness::record("/a", &bar),
            UpdateCollectionEntriesUseCaseHarness::record("/b", &baz),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_several_changes_to_same_path_in_one_update() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, bar, _] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                UpdateCollectionEntriesUseCaseHarness::add("/a", &foo),
                UpdateCollectionEntriesUseCaseHarness::mv("/a", "/b"),
                UpdateCollectionEntriesUseCaseHarness::add("/a", &bar),
            ],
        )
        .await;

    assert_eq!(
        harness.ledger(&collection).await,
        vec![
            (0, Op::Append, "/a".to_string(), foo.clone()),
            (1, Op::Retract, "/a".to_string(), foo.clone()),
            (2, Op::Append, "/b".to_string(), foo.clone()),
            (3, Op::Append, "/a".to_string(), bar.clone()),
        ]
    );

    assert_eq!(
        harness.entries(&collection).await,
        vec![
            UpdateCollectionEntriesUseCaseHarness::record("/a", &bar),
            UpdateCollectionEntriesUseCaseHarness::record("/b", &foo),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_move_in_place_corrects_extra_data() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness
        .create_collection(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    harness
        .update_success(
            &collection,
            vec![CollectionUpdateOperation::add(
                CollectionPath::new("/a".to_string()),
                foo.clone(),
                UpdateCollectionEntriesUseCaseHarness::tag("x"),
            )],
        )
        .await;

    let res = harness
        .update_success(
            &collection,
            vec![CollectionUpdateOperation::r#move(
                CollectionPath::new("/a".to_string()),
                CollectionPath::new("/a".to_string()),
                Some(UpdateCollectionEntriesUseCaseHarness::tag("y")),
            )],
        )
        .await;

    let record_x = CollectionEntryRecord {
        extra_data: UpdateCollectionEntriesUseCaseHarness::tag("x"),
        ..UpdateCollectionEntriesUseCaseHarness::record("/a", &foo)
    };
    let record_y = CollectionEntryRecord {
        extra_data: UpdateCollectionEntriesUseCaseHarness::tag("y"),
        ..UpdateCollectionEntriesUseCaseHarness::record("/a", &foo)
    };

    assert_eq!(
        res.inserted_records,
        vec![
            (Op::CorrectFrom, record_x),
            (Op::CorrectTo, record_y.clone()),
        ]
    );
    assert_eq!(harness.entries(&collection).await, vec![record_y]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_without_changes_is_up_to_date() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![UpdateCollectionEntriesUseCaseHarness::add("/a", &foo)],
        )
        .await;
    let chain_len = harness.chain_len(&collection).await;

    // Identical entry, a path that does not exist, and no operations at all
    for operations in [
        vec![UpdateCollectionEntriesUseCaseHarness::add("/a", &foo)],
        vec![UpdateCollectionEntriesUseCaseHarness::remove("/missing")],
        vec![],
    ] {
        let res = harness.update(&collection, operations, None).await;
        assert_matches!(res, Ok(UpdateCollectionEntriesResult::UpToDate));
    }

    assert_eq!(harness.chain_len(&collection).await, chain_len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_move_from_missing_path_is_not_found() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;
    let chain_len = harness.chain_len(&collection).await;

    // The valid operation before the failing one is not committed either
    let res = harness
        .update(
            &collection,
            vec![
                UpdateCollectionEntriesUseCaseHarness::add("/a", &foo),
                UpdateCollectionEntriesUseCaseHarness::mv("/missing", "/b"),
            ],
            None,
        )
        .await;

    assert_matches!(
        res,
        Ok(UpdateCollectionEntriesResult::NotFound(e)) if e.path.as_str() == "/missing"
    );
    assert_eq!(harness.chain_len(&collection).await, chain_len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_stale_expected_head_fails() {
    let harness = UpdateCollectionEntriesUseCaseHarness::new();
    let [foo, bar, _] = UpdateCollectionEntriesUseCaseHarness::linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    let res = harness
        .update_success(
            &collection,
            vec![UpdateCollectionEntriesUseCaseHarness::add("/a", &foo)],
        )
        .await;
    let stale_head = res.old_head;

    let res = harness
        .update(
            &collection,
            vec![UpdateCollectionEntriesUseCaseHarness::add("/b", &bar)],
            Some(stale_head),
        )
        .await;

    assert_matches!(
        res,
        Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_))
    );
    assert_eq!(
        harness.entries(&collection).await,
        vec![UpdateCollectionEntriesUseCaseHarness::record("/a", &foo)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateCollectionEntriesUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    update_use_case: Arc<dyn UpdateCollectionEntriesUseCase>,
    view_use_case: Arc<dyn ViewCollectionEntriesUseCase>,
    query_svc: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl UpdateCollectionEntriesUseCaseHarness {
    fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .without_outbox()
                .with_maybe_authorizer(Some(MockDatasetActionAuthorizer::new())),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        let catalog = b
            .add::<UpdateCollectionEntriesUseCaseImpl>()
            .add::<ViewCollectionEntriesUseCaseImpl>()
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
            view_use_case: catalog.get_one().unwrap(),
            query_svc: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
        }
    }

    /// Datasets the entries link to; the use case does not require them to exist
    fn linked_datasets() -> [odf::DatasetID; 3] {
        [b"foo", b"bar", b"baz"].map(|seed| odf::DatasetID::new_seeded_ed25519(seed))
    }

    fn add(path: &str, reference: &odf::DatasetID) -> CollectionUpdateOperation {
        CollectionUpdateOperation::add(
            CollectionPath::new(path.to_string()),
            reference.clone(),
            ExtraDataFields::default(),
        )
    }

    fn mv(path_from: &str, path_to: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::r#move(
            CollectionPath::new(path_from.to_string()),
            CollectionPath::new(path_to.to_string()),
            None,
        )
    }

    fn remove(path: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::remove(CollectionPath::new(path.to_string()))
    }

    fn record(path: &str, reference: &odf::DatasetID) -> CollectionEntryRecord {
        CollectionEntryRecord {
            path: CollectionPath::new(path.to_string()),
            reference: reference.clone(),
            extra_data: ExtraDataFields::default(),
        }
    }

    fn tag(value: &str) -> ExtraDataFields {
        ExtraDataFields::new(
            serde_json::json!({ "tag": value })
                .as_object()
                .unwrap()
                .clone(),
        )
    }

    async fn create_collection(
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

    async fn update(
        &self,
        collection: &ResolvedDataset,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError> {
        self.update_use_case
            .execute(
                WriteCheckedDataset::from_ref(collection),
                None,
                operations,
                expected_head,
            )
            .await
    }

    async fn update_success(
        &self,
        collection: &ResolvedDataset,
        operations: Vec<CollectionUpdateOperation>,
    ) -> UpdateCollectionEntriesSuccess {
        let res = self.update(collection, operations, None).await.unwrap();
        let UpdateCollectionEntriesResult::Success(success) = res else {
            panic!("Expected a successful update, got: {res:?}");
        };
        success
    }

    async fn chain_len(&self, collection: &ResolvedDataset) -> u64 {
        use odf::dataset::MetadataChainExt;

        let head = collection
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await
            .unwrap();
        head.sequence_number + 1
    }

    /// Raw changelog records as `(offset, op, path, ref)`, ordered by offset
    async fn ledger(&self, collection: &ResolvedDataset) -> Vec<(u64, Op, String, odf::DatasetID)> {
        let df = self
            .query_svc
            .get_data(collection.clone(), GetDataOptions::default())
            .await
            .unwrap()
            .df
            .unwrap();

        let mut ledger: Vec<_> = df
            .collect_json_aos()
            .await
            .unwrap()
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

    /// Current entries, ordered by path
    async fn entries(&self, collection: &ResolvedDataset) -> Vec<CollectionEntryRecord> {
        let mut entries: Vec<CollectionEntryRecord> = self
            .view_use_case
            .execute(
                ReadCheckedDataset::from_ref(collection),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .list
            .into_iter()
            .map(Into::into)
            .collect();

        entries.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        entries
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
