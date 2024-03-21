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
use dill::Component;
use domain::compact_service::{CompactError, CompactService, NullCompactionMultiListener};
use event_bus::EventBus;
use futures::TryStreamExt;
use kamu::domain::*;
use kamu::testing::{DatasetTestHelper, MetadataFactory};
use kamu::*;
use kamu_core::{auth, CurrentAccountSubject};
use opendatafabric::*;

const FILE_DATA_ARRAY_SIZE: usize = 32;
const MAX_SLICE_SIZE: u64 = 1024 * 1024 * 1024;

#[tokio::test]
async fn test_dataset_compact() {
    let harness = CompactTestHarness::new();

    let root_dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let derive_dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("derive-foo"));

    harness
        .dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await
        .unwrap();

    let head = DatasetTestHelper::append_random_data(
        harness.dataset_repo.as_ref(),
        root_dataset_alias.as_local_ref(),
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let dataset_handle = harness
        .dataset_repo
        .resolve_dataset_ref(&root_dataset_alias.as_local_ref())
        .await
        .unwrap();

    let dataset = harness
        .dataset_repo
        .get_dataset(&dataset_handle.as_local_ref())
        .await
        .unwrap();

    let old_blocks: Vec<_> = dataset
        .as_metadata_chain()
        .iter_blocks_interval(&head, None, false)
        .try_collect()
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Ok(()),
    );

    let new_blocks: Vec<_> = dataset
        .as_metadata_chain()
        .iter_blocks_interval(&head, None, false)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(old_blocks.len(), new_blocks.len());

    harness
        .dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("derive-foo")
                .kind(DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(["foo"])
                        .build(),
                )
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await
        .unwrap();

    let dataset_handle = harness
        .dataset_repo
        .resolve_dataset_ref(&derive_dataset_alias.as_local_ref())
        .await
        .unwrap();

    assert_matches!(
        harness
            .compact_svc
            .compact_dataset(
                &dataset_handle,
                MAX_SLICE_SIZE,
                Some(Arc::new(NullCompactionMultiListener {}))
            )
            .await,
        Err(CompactError::InvalidDatasetKind(_)),
    );
}

struct CompactTestHarness {
    dataset_repo: Arc<dyn DatasetRepository>,
    compact_svc: Arc<dyn CompactService>,
}

impl CompactTestHarness {
    fn new() -> Self {
        Self::new_with_authorizer(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new())
    }

    fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        dataset_action_authorizer: TDatasetAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(cache_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(temp_dir.path().join("datasets"))
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add_builder(CompactServiceImpl::builder().with_run_info_dir(run_info_dir))
            .bind::<dyn CompactService, CompactServiceImpl>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let compact_svc = catalog.get_one::<dyn CompactService>().unwrap();

        Self {
            dataset_repo,
            compact_svc,
        }
    }
}
