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

use chrono::{TimeZone, Utc};
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_core::MockDidGenerator;
use kamu_datasets::CommitDatasetEventUseCase;
use kamu_datasets_services::CommitDatasetEventUseCaseImpl;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceStub;

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_dataset_event() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, true);

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo.clone()]),
    )
    .await;
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.reset_collected_outbox_messages();

    let foo_old_head = foo.head.clone();

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
        )
        .await;
    assert_matches!(res, Ok(_));

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            "#
        )
        .replace("<foo_id>", dataset_id_foo.to_string().as_str())
        .replace("<old_head>", foo_old_head.to_string().as_str())
        .replace("<new_head>", res.unwrap().new_head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&dataset_id_foo, 1, false);

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo]),
    )
    .await;
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.reset_collected_outbox_messages();

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
        )
        .await;
    assert_matches!(res, Err(odf::dataset::CommitError::Access(_)));

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages(),);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_with_same_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer = MockDatasetActionAuthorizer::new()
        .expect_check_write_dataset(&dataset_id_bar, 1, true)
        .expect_check_read_dataset(&dataset_id_foo, 1, true);

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![dataset_id_foo, dataset_id_bar.clone()]),
    )
    .await;

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    let bar_old_head = bar.head.clone();

    let res = harness
        .use_case
        .execute(
            &bar.dataset_handle,
            odf::MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        foo.dataset_handle.id,
                        alias_foo.to_string(),
                    )])
                    .build(),
            ),
        )
        .await;
    assert_matches!(res, Ok(_));

    // No dependency updates happened
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <bar_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            "#
        )
        .replace("<bar_id>", dataset_id_bar.to_string().as_str())
        .replace("<old_head>", bar_old_head.to_string().as_str())
        .replace("<new_head>", res.unwrap().new_head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_with_new_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_bar) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_baz) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer = MockDatasetActionAuthorizer::allowing();

    let harness = CommitDatasetEventUseCaseHarness::new(
        mock_authorizer,
        MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo.clone(),
            dataset_id_bar.clone(),
            dataset_id_baz.clone(),
        ]),
    )
    .await;

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness
        .create_derived_dataset(&alias_baz, vec![foo.dataset_handle.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    let baz_old_head = baz.head.clone();

    let res = harness
        .use_case
        .execute(
            &baz.dataset_handle,
            odf::MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        bar.dataset_handle.id,
                        alias_bar.to_string(),
                    )])
                    .build(),
            ),
        )
        .await;
    assert_matches!(res, Ok(_));

    // No dependency updates happened
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <baz_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            Dataset Dependency Messages: 1
              Deps Updated {
                Dataset ID: <baz_id>
                Added: [<bar_id>]
                Obsolete: [<foo_id>]
              }
            "#
        )
        .replace("<foo_id>", dataset_id_foo.to_string().as_str())
        .replace("<bar_id>", dataset_id_bar.to_string().as_str())
        .replace("<baz_id>", dataset_id_baz.to_string().as_str())
        .replace("<old_head>", baz_old_head.to_string().as_str())
        .replace("<new_head>", res.unwrap().new_head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct CommitDatasetEventUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn CommitDatasetEventUseCase>,
}

impl CommitDatasetEventUseCaseHarness {
    async fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_did_generator: MockDidGenerator,
    ) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_system_time_source_stub: Some(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                )),
                maybe_mock_dataset_action_authorizer: Some(mock_dataset_action_authorizer),
                maybe_mock_did_generator: Some(mock_did_generator),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog = dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.catalog())
            .add::<CommitDatasetEventUseCaseImpl>()
            .build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
