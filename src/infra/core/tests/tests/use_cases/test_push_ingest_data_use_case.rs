// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions, MockDatasetActionAuthorizer};
use kamu::*;
use kamu_core::*;
use messaging_outbox::{register_message_dispatcher, DummyOutboxImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_ingest_data_source_not_found() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetActionAuthorizer::new(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness.create_root_dataset(&alias_foo).await;

    let data_stream = std::io::Cursor::new("{}");

    assert_matches!(
        harness
            .ingest_data(
                &ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(Box::new(data_stream)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: None,
                }
            )
            .await,
        Err(PushIngestDataError::Planning(
            PushIngestPlanningError::SourceNotFound(_)
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_from_json() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetActionAuthorizer::new(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let data_stream = std::io::Cursor::new("{\"city\":\"foo\", \"population\":100}");

    assert_matches!(
        harness
            .ingest_data(
                &ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(Box::new(data_stream)),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: false,
                    media_type: None,
                }
            )
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[test_group::group(engine, ingest, datafusion)]
async fn test_push_ingest_data_from_file() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = PushIngestDataUseCaseHarness::new(
        MockDatasetActionAuthorizer::new(),
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    );

    let create_dataset_result = harness
        .create_root_dataset_with_push_source(&alias_foo)
        .await;

    let data_file_content = "{\"city\":\"foo\", \"population\":100}";

    let data_file_path = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(data_file_path.path(), data_file_content)
        .await
        .unwrap();

    let file = tokio::fs::File::open(data_file_path.path()).await.unwrap();
    let data_stream = Box::new(file);

    assert_matches!(
        harness
            .ingest_data(
                &ResolvedDataset::from_created(&create_dataset_result),
                DataSource::Stream(data_stream),
                PushIngestDataUseCaseOptions {
                    source_name: None,
                    source_event_time: None,
                    is_ingest_from_upload: true,
                    media_type: Some(MediaType::NDJSON.to_owned())
                }
            )
            .await,
        Ok(PushIngestResult::Updated { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct PushIngestDataUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn PushIngestDataUseCase>,
}

impl PushIngestDataUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .with_maybe_mock_did_generator(Some(mock_did_generator))
                .without_outbox(),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());

        b.add::<PushIngestDataUseCaseImpl>()
            .add::<PushIngestPlannerImpl>()
            .add::<PushIngestExecutorImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<DummyOutboxImpl>()
            .add_value(EngineConfigDatafusionEmbeddedIngest::default())
            .add::<EngineProvisionerNull>();

        register_message_dispatcher::<kamu_datasets::DatasetExternallyChangedMessage>(
            &mut b,
            kamu_datasets::MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
        );

        let catalog = b.build();

        let use_case = catalog.get_one::<dyn PushIngestDataUseCase>().unwrap();

        Self {
            base_use_case_harness,
            use_case,
        }
    }

    async fn ingest_data(
        &self,
        target: &ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.use_case
            .execute(target, data_source, options, None)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
