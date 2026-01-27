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
use kamu_core::MockDidGenerator;
use kamu_datasets::{DeleteDatasetError, DeleteDatasetUseCase};
use kamu_datasets_services::DeleteDatasetUseCaseImpl;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use time_source::{SystemTimeSourceProvider, SystemTimeSourceStub};

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_ref() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_own_dataset(&foo_id, 1, true);

    let harness = DeleteUseCaseHarness::new(
        mock_authorizer,
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            foo_id.clone(),
        ])),
    )
    .await;

    harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    harness
        .use_case
        .execute_via_ref(&alias_foo.as_local_ref())
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", foo_id.to_string().as_str()),
        harness.collected_outbox_messages()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_success_via_handle() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_own_dataset(&foo_id, 1, true);

    let harness = DeleteUseCaseHarness::new(
        mock_authorizer,
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            foo_id.clone(),
        ])),
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    harness
        .use_case
        .execute_via_handle(&foo.dataset_handle)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", foo_id.to_string().as_str()),
        harness.collected_outbox_messages()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_not_found() {
    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::new(), None).await;

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute_via_ref(&alias_foo.as_local_ref())
            .await,
        Err(DeleteDatasetError::NotFound(_))
    );

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = DeleteUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_own_dataset(&dataset_id_foo, 1, false),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    )
    .await;

    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    assert_matches!(
        harness
            .use_case
            .execute_via_handle(&foo.dataset_handle)
            .await,
        Err(DeleteDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_dataset_respects_dangling_refs() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let harness = DeleteUseCaseHarness::new(MockDatasetActionAuthorizer::allowing(), None).await;

    let root = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    let derived = harness
        .create_derived_dataset(&harness.catalog, &alias_bar, vec![alias_foo.as_local_ref()])
        .await;
    harness.reset_collected_outbox_messages();

    assert_matches!(
        harness.use_case.execute_via_handle(&root.dataset_handle).await,
        Err(DeleteDatasetError::DanglingReference(e)) if e.children == vec![derived.dataset_handle.clone()]
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    harness
        .use_case
        .execute_via_handle(&derived.dataset_handle)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    harness
        .use_case
        .execute_via_handle(&root.dataset_handle)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 2
              Deleted {
                Dataset ID: {bar_id}
              }
              Deleted {
                Dataset ID: {foo_id}
              }
            "#
        )
        .replace("{foo_id}", root.dataset_handle.id.to_string().as_str())
        .replace("{bar_id}", derived.dataset_handle.id.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct DeleteUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn DeleteDatasetUseCase>,
    catalog: dill::Catalog,
}

impl DeleteUseCaseHarness {
    async fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_provider: SystemTimeSourceProvider::Stub(
                    SystemTimeSourceStub::new_set(
                        Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                    ),
                ),
                maybe_mock_did_generator,
                maybe_mock_dataset_action_authorizer: Some(mock_dataset_action_authorizer),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog())
                .add::<DeleteDatasetUseCaseImpl>()
                .build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
