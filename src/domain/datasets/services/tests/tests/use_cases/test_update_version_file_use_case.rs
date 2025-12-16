// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::io::Cursor;
use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions, MockDatasetActionAuthorizer};
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
use kamu_core::{DidGenerator, FileUploadLimitConfig, MockDidGenerator};
use kamu_datasets::{
    ContentArgs,
    DatasetRegistry,
    ResolvedDataset,
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
    WriteCheckedDataset,
};
use kamu_datasets_inmem::InMemoryDatasetStatisticsRepository;
use kamu_datasets_services::{
    DatasetStatisticsServiceImpl,
    QuotaCheckerStorageImpl,
    UpdateVersionedFileUseCaseImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::dataset::{MetadataChainExt, TryStreamExtExt};
use time_source::SystemTimeSource;
use tokio::io::BufReader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_use_case() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness =
        UpdateVersionFileCaseHarness::new(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ]));

    let file_dataset = harness.create_versioned_file(&alias_foo).await;

    let content_args = UpdateVersionFileCaseHarness::combine_content_args(b"foo");
    let content_hash = content_args.content_hash.clone();

    // Upload first version
    let res = harness
        .use_case
        .execute(
            WriteCheckedDataset::from_ref(&file_dataset),
            None,
            Some(content_args),
            None,
            None,
        )
        .await;

    assert_matches!(res, Ok(result) if result.new_version == 1 && result.content_hash == content_hash);

    let content_args = UpdateVersionFileCaseHarness::combine_content_args(b"foo_new");
    let content_hash = content_args.content_hash.clone();

    // Upload second version
    let res = harness
        .use_case
        .execute(
            WriteCheckedDataset::from_ref(&file_dataset),
            None,
            Some(content_args),
            None,
            None,
        )
        .await;

    assert_matches!(res, Ok(result) if result.new_version == 2 && result.content_hash == content_hash);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_use_case_errors() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness =
        UpdateVersionFileCaseHarness::new(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ]));

    let file_dataset = harness.create_versioned_file(&alias_foo).await;

    let content_args = UpdateVersionFileCaseHarness::combine_content_args(b"foo");

    let seed_bloch_hash = harness.get_seed_block_hash(file_dataset.as_ref()).await;

    let res = harness
        .use_case
        .execute(
            WriteCheckedDataset::from_ref(&file_dataset),
            None,
            Some(content_args),
            Some(seed_bloch_hash),
            None,
        )
        .await;
    let old_head = res.as_ref().unwrap().old_head.clone();

    assert_matches!(res, Ok(result) if result.new_version == 1);

    let content_args = UpdateVersionFileCaseHarness::combine_content_args(b"bar");

    // RefCASFailed error
    let res = harness
        .use_case
        .execute(
            WriteCheckedDataset::from_ref(&file_dataset),
            None,
            Some(content_args),
            Some(old_head),
            None,
        )
        .await;

    assert_matches!(res, Err(UpdateVersionFileUseCaseError::RefCASFailed(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateVersionFileCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn UpdateVersionedFileUseCase>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl UpdateVersionFileCaseHarness {
    fn new(mock_did_generator: MockDidGenerator) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .without_outbox()
                .with_maybe_authorizer(Some(MockDatasetActionAuthorizer::new()))
                .with_maybe_mock_did_generator(Some(mock_did_generator)),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        let catalog = b
            .add::<UpdateVersionedFileUseCaseImpl>()
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
            .add::<QuotaCheckerStorageImpl>()
            .add::<QueryServiceImpl>()
            .add::<SessionContextBuilder>()
            .add_value(FileUploadLimitConfig::new_in_bytes(24))
            .add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
            .build();

        Self {
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            did_generator: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
        }
    }

    pub async fn create_versioned_file(
        &self,
        dataset_alias: &odf::DatasetAlias,
    ) -> ResolvedDataset {
        let push_source = odf::metadata::AddPushSource {
            source_name: "default".into(),
            read: odf::metadata::ReadStep::NdJson(odf::metadata::ReadStepNdJson {
                schema: Some(
                    [
                        "version INT",
                        "content_hash STRING",
                        "content_length BIGINT",
                        "content_type STRING",
                    ]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                ),
                ..Default::default()
            }),
            preprocess: None,
            merge: odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
        };

        let snapshot = odf::DatasetSnapshot {
            name: dataset_alias.clone(),
            kind: odf::DatasetKind::Root,
            metadata: [odf::MetadataEvent::AddPushSource(push_source)]
                .into_iter()
                .collect(),
        };

        let store_result = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            self.did_generator.generate_dataset_id().0,
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        ResolvedDataset::from_stored(&store_result, dataset_alias)
    }

    pub async fn get_seed_block_hash(&self, dataset: &dyn odf::Dataset) -> odf::Multihash {
        let seed_block = dataset
            .as_metadata_chain()
            .iter_blocks()
            .try_first()
            .await
            .unwrap()
            .unwrap();

        seed_block.0
    }

    fn combine_content_args(content: &[u8]) -> ContentArgs {
        let content_hash = odf::Multihash::from_digest_sha3_256(content);
        let reader = BufReader::new(Cursor::new(content.to_vec()));

        ContentArgs {
            content_stream: Some(Box::new(reader)),
            content_hash: content_hash.clone(),
            content_length: content.len(),
            content_type: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
