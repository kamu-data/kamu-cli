// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::{
    domain::{CommitOpts, DatasetExt, DatasetRepositoryExt, PullResult},
    testing::MetadataFactory,
};
use opendatafabric::{
    DatasetKind, DatasetName, DatasetRefAny, DatasetRefLocal, DatasetRefRemote, MetadataEvent,
};
use std::{fs, io, path::Path, time};

use crate::harness::{client_side_harness, server_side_harness};

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! await_client_server_flow {
    ($api_server_handle: expr, $client_handle: expr) => {
        tokio::select! {
            _ = tokio::time::sleep(time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "skip_docker_tests", ignore)]
#[test_log::test(tokio::test)]
async fn test_smart_pull_new_dataset() {
    let server_harness = server_side_harness();

    let server_repo = server_harness.dataset_repository();
    let create_result = server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_harness = client_side_harness();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = async { server_harness.api_server_run().await };
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset(DatasetRefAny::from(foo_dataset_ref))
            .await;
        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: create_result.head,
                num_blocks: 2
            },
            pull_result
        )
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "skip_docker_tests", ignore)]
#[test_log::test(tokio::test)]
async fn test_smart_pull_existing_up_to_date_dataset() {
    let server_harness = server_side_harness();

    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_harness = client_side_harness();

    // Hard folder synchronization
    copy_folder_recursively(
        server_harness
            .internal_bucket_folder_path()
            .join("foo")
            .as_path(),
        client_harness
            .internal_datasets_folder_path()
            .join("foo")
            .as_path(),
    )
    .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = async { server_harness.api_server_run().await };
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset(DatasetRefAny::from(foo_dataset_ref))
            .await;
        assert_eq!(PullResult::UpToDate {}, pull_result)
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "skip_docker_tests", ignore)]
#[test_log::test(tokio::test)]
async fn test_smart_pull_existing_evolved_dataset() {
    let server_harness = server_side_harness();

    let server_repo = server_harness.dataset_repository();
    let create_result = server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_harness = client_side_harness();

    // Hard folder synchronization
    copy_folder_recursively(
        server_harness
            .internal_bucket_folder_path()
            .join("foo")
            .as_path(),
        client_harness
            .internal_datasets_folder_path()
            .join("foo")
            .as_path(),
    )
    .unwrap();

    // Extend server-side dataset with new node
    let commit_result = server_repo
        .get_dataset(&DatasetRefLocal::from(
            DatasetName::try_from("foo").unwrap(),
        ))
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .description("updated description")
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = async { server_harness.api_server_run().await };
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset(DatasetRefAny::from(foo_dataset_ref))
            .await;
        assert_eq!(
            PullResult::Updated {
                old_head: Some(create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 1
            },
            pull_result
        )
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

#[ignore = "Error handling in Smart Transfer Protocol to be completely revised"]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
#[test_log::test(tokio::test)]
async fn test_smart_pull_existing_advanced_dataset_fails() {
    let server_harness = server_side_harness();

    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_harness = client_side_harness();

    // Hard folder synchronization
    copy_folder_recursively(
        server_harness
            .internal_bucket_folder_path()
            .join("foo")
            .as_path(),
        client_harness
            .internal_datasets_folder_path()
            .join("foo")
            .as_path(),
    )
    .unwrap();

    // Extend client-side dataset with new node
    let client_repo = client_harness.dataset_repository();
    client_repo
        .get_dataset(&DatasetRefLocal::from(
            DatasetName::try_from("foo").unwrap(),
        ))
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .description("updated description")
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = async { server_harness.api_server_run().await };
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset(DatasetRefAny::from(foo_dataset_ref))
            .await;
        assert_eq!(PullResult::UpToDate {}, pull_result)
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

fn copy_folder_recursively(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_folder_recursively(&entry.path(), &dst.join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.join(entry.file_name()))?;
        }
    }
    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////
