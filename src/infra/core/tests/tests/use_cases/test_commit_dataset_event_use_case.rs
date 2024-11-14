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

use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::CommitDatasetEventUseCaseImpl;
use kamu_core::{CommitDatasetEventUseCase, CommitError, CommitOpts};
use messaging_outbox::MockOutbox;
use opendatafabric::{DatasetAlias, DatasetName, MetadataEvent};

use crate::tests::use_cases::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_dataset_event() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, true);

    let mock_outbox = MockOutbox::new();

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_unauthorized() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_foo, 1, false);

    let mock_outbox = MockOutbox::new();

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let foo = harness.create_root_dataset(&alias_foo).await;

    let res = harness
        .use_case
        .execute(
            &foo.dataset_handle,
            MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Err(CommitError::Access(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_event_with_new_dependencies() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mock_authorizer =
        MockDatasetActionAuthorizer::new().expect_check_write_dataset(&alias_bar, 1, true);

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_dependencies_updated(&mut mock_outbox, 1);

    let harness = CommitDatasetEventUseCaseHarness::new(mock_authorizer, mock_outbox);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let res = harness
        .use_case
        .execute(
            &bar.dataset_handle,
            MetadataEvent::SetTransform(
                MetadataFactory::set_transform()
                    .inputs_from_refs_and_aliases(vec![(
                        foo.dataset_handle.id,
                        alias_foo.to_string(),
                    )])
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct CommitDatasetEventUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn CommitDatasetEventUseCase>,
}

impl CommitDatasetEventUseCaseHarness {
    fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        mock_outbox: MockOutbox,
    ) -> Self {
        let base_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_authorizer(mock_dataset_action_authorizer)
                .with_outbox(mock_outbox),
        );

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<CommitDatasetEventUseCaseImpl>()
            .build();

        let use_case = catalog.get_one::<dyn CommitDatasetEventUseCase>().unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
