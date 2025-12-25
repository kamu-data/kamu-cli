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
use kamu_datasets::{RenameDatasetError, RenameDatasetUseCase};
use kamu_datasets_services::RenameDatasetUseCaseImpl;
use time_source::{SystemTimeSourceHarnessMode, SystemTimeSourceStub};

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_success_via_ref() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let (_, foo_id) = odf::DatasetID::new_generated_ed25519();

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_maintain_dataset(&foo_id, 1, true);

    let harness = RenameUseCaseHarness::new(
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

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(
        harness.check_dataset_exists(&alias_bar).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );

    harness
        .use_case
        .execute(&alias_foo.as_local_ref(), &alias_bar.dataset_name)
        .await
        .unwrap();

    assert_matches!(
        harness.check_dataset_exists(&alias_foo).await,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Renamed {
                Dataset ID: <foo_id>
                New Name: bar
              }
            "#
        )
        .replace("<foo_id>", foo_id.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_not_found() {
    let harness = RenameUseCaseHarness::new(MockDatasetActionAuthorizer::new(), None).await;

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &odf::DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::NotFound(_))
    );

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages(),);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset_unauthorized() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let (_, dataset_id_foo) = odf::DatasetID::new_generated_ed25519();

    let harness = RenameUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().expect_check_maintain_dataset(&dataset_id_foo, 1, false),
        Some(MockDidGenerator::predefined_dataset_ids(vec![
            dataset_id_foo,
        ])),
    )
    .await;

    harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    assert_matches!(
        harness
            .use_case
            .execute(
                &alias_foo.as_local_ref(),
                &odf::DatasetName::new_unchecked("bar")
            )
            .await,
        Err(RenameDatasetError::Access(_))
    );

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));

    pretty_assertions::assert_eq!("", harness.collected_outbox_messages(),);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct RenameUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn RenameDatasetUseCase>,
    catalog: dill::Catalog,
}

impl RenameUseCaseHarness {
    async fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        maybe_mock_did_generator: Option<MockDidGenerator>,
    ) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_harness_mode: SystemTimeSourceHarnessMode::Stub(
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
                .add::<RenameDatasetUseCaseImpl>()
                .build();

        let use_case = catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

        Self {
            dataset_base_use_case_harness,
            use_case,
            catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
