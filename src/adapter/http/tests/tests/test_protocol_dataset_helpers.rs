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

use axum_extra::TypedHeader;
use headers::Header;
use kamu::domain::Dataset;
use kamu::testing::{MetadataFactory, TEST_BUCKET_NAME};
use kamu_accounts::DUMMY_ACCESS_TOKEN;
use kamu_adapter_http::smart_protocol::messages::{self, SMART_TRANSFER_PROTOCOL_VERSION};
use kamu_adapter_http::smart_protocol::protocol_dataset_helper::*;
use kamu_adapter_http::{BearerHeader, OdfSmtpVersion};
use kamu_core::TenancyConfig;
use opendatafabric::{DatasetID, DatasetKind, Multihash};
use url::Url;

use crate::harness::{
    commit_add_data_event,
    make_dataset_ref,
    ServerSideHarness,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
    ServerSideS3Harness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_object_url_local_fs() {
    let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
        tenancy_config: TenancyConfig::SingleTenant,
        authorized_writes: true,
        base_catalog: None,
    })
    .await;

    let test_case = create_test_case(&server_harness).await;

    let pull_strategy_slice = prepare_pull_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.data_slice_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let pull_strategy_checkpoint = prepare_pull_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.checkpoint_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_existing_slice = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.data_slice_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_existing_checkpoint = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.checkpoint_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_new_slice = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        &messages::ObjectFileReference {
            object_type: messages::ObjectType::DataSlice,
            physical_hash: Multihash::from_digest_sha3_256(b"new-slice"),
            size: 12345,
        },
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_new_checkpoint = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        &messages::ObjectFileReference {
            object_type: messages::ObjectType::Checkpoint,
            physical_hash: Multihash::from_digest_sha3_256(b"new-checkpoint"),
            size: 321,
        },
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    assert_matches!(
        pull_strategy_slice,
        messages::PullObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            pull_strategy: messages::ObjectPullStrategy::HttpDownload,
            download_from: messages::TransferUrl {
                url: download_from_url,
                headers: download_from_headers,
                ..
            }
        } if
            object_type == messages::ObjectType::DataSlice &&
            physical_hash == test_case.data_slice_object().physical_hash &&
            download_from_url == test_case.dataset_url.join(format!("data/{}", physical_hash.as_multibase()).as_str()).unwrap() &&
            download_from_headers == vec![
                messages::HeaderRow {
                    name: OdfSmtpVersion::name().to_string(),
                    value: SMART_TRANSFER_PROTOCOL_VERSION.to_string(),
                },
                messages::HeaderRow {
                    name: http::header::AUTHORIZATION.to_string(),
                    value: format!("Bearer {DUMMY_ACCESS_TOKEN}")
                }
            ]
    );

    assert_matches!(
        pull_strategy_checkpoint,
        messages::PullObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            pull_strategy: messages::ObjectPullStrategy::HttpDownload,
            download_from: messages::TransferUrl {
                url: download_from_url,
                headers: download_from_headers,
                ..
            }
        } if
            object_type == messages::ObjectType::Checkpoint &&
            physical_hash == test_case.checkpoint_object().physical_hash &&
            download_from_url == test_case.dataset_url.join(format!("checkpoints/{}", physical_hash.as_multibase()).as_str()).unwrap() &&
            download_from_headers == vec![
                messages::HeaderRow {
                    name: OdfSmtpVersion::name().to_string(),
                    value: SMART_TRANSFER_PROTOCOL_VERSION.to_string(),
                },
                messages::HeaderRow {
                    name: http::header::AUTHORIZATION.to_string(),
                    value: format!("Bearer {DUMMY_ACCESS_TOKEN}", )
                }
            ]
    );

    assert_matches!(
        push_strategy_existing_slice,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type: messages::ObjectType::DataSlice,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::SkipUpload,
            upload_to: None
        }
    );

    assert_matches!(
        push_strategy_existing_checkpoint,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type: messages::ObjectType::Checkpoint,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::SkipUpload,
            upload_to: None
        }
    );

    assert_matches!(
        push_strategy_new_slice,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::HttpUpload,
            upload_to: Some(messages::TransferUrl {
                url: upload_to_url,
                headers: upload_to_headers,
                ..
            })
        } if
            object_type == messages::ObjectType::DataSlice &&
            upload_to_url == test_case.dataset_url.join(format!("data/{}", physical_hash.as_multibase()).as_str()).unwrap() &&
            upload_to_headers == vec![
                messages::HeaderRow {
                    name: OdfSmtpVersion::name().to_string(),
                    value: SMART_TRANSFER_PROTOCOL_VERSION.to_string(),
                },
                messages::HeaderRow {
                    name: http::header::AUTHORIZATION.to_string(),
                    value: format!("Bearer {DUMMY_ACCESS_TOKEN}", )
                }
            ]
    );

    assert_matches!(
        push_strategy_new_checkpoint,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::HttpUpload,
            upload_to: Some(messages::TransferUrl {
                url: upload_to_url,
                headers: upload_to_headers,
                ..
            })
        } if
            object_type == messages::ObjectType::Checkpoint &&
            upload_to_url == test_case.dataset_url.join(format!("checkpoints/{}", physical_hash.as_multibase()).as_str()).unwrap() &&
            upload_to_headers == vec![
                messages::HeaderRow {
                    name: OdfSmtpVersion::name().to_string(),
                    value: SMART_TRANSFER_PROTOCOL_VERSION.to_string(),
                },
                messages::HeaderRow {
                    name: http::header::AUTHORIZATION.to_string(),
                    value: format!("Bearer {DUMMY_ACCESS_TOKEN}", )
                }
            ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_pull_object_url_s3() {
    let server_harness = ServerSideS3Harness::new(ServerSideHarnessOptions {
        tenancy_config: TenancyConfig::SingleTenant,
        authorized_writes: true,
        base_catalog: None,
    })
    .await;

    let test_case = create_test_case(&server_harness).await;

    let transfer_strategy_slice = prepare_pull_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.data_slice_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let transfer_strategy_checkpoint = prepare_pull_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.checkpoint_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_existing_slice = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.data_slice_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_existing_checkpoint = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        test_case.checkpoint_object(),
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_new_slice = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        &messages::ObjectFileReference {
            object_type: messages::ObjectType::DataSlice,
            physical_hash: Multihash::from_digest_sha3_256(b"new-slice"),
            size: 12345,
        },
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    let push_strategy_new_checkpoint = prepare_push_object_transfer_strategy(
        test_case.dataset.as_ref(),
        &messages::ObjectFileReference {
            object_type: messages::ObjectType::Checkpoint,
            physical_hash: Multihash::from_digest_sha3_256(b"new-checkpoint"),
            size: 321,
        },
        &test_case.dataset_url,
        Some(test_case.bearer_header.clone()).as_ref(),
    )
    .await
    .unwrap();

    assert_matches!(
        transfer_strategy_slice,
        messages::PullObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            pull_strategy: messages::ObjectPullStrategy::HttpDownload,
            download_from: messages::TransferUrl {
                url: download_from_url,
                headers: download_from_headers,
                ..
            }
        } if
            object_type == messages::ObjectType::DataSlice &&
            physical_hash == test_case.data_slice_object().physical_hash &&
            download_from_url.path() == format!(
                "/{}/{}/data/{}",
                TEST_BUCKET_NAME,
                test_case.dataset_id.as_multibase(),
                physical_hash.as_multibase()
            ).as_str() &&
            download_from_headers == vec![]
    );

    assert_matches!(
        transfer_strategy_checkpoint,
        messages::PullObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            pull_strategy: messages::ObjectPullStrategy::HttpDownload,
            download_from: messages::TransferUrl {
                url: download_from_url,
                headers: download_from_headers,
                ..
            }
        } if
            object_type == messages::ObjectType::Checkpoint &&
            physical_hash == test_case.checkpoint_object().physical_hash &&
            download_from_url.path() == format!(
                "/{}/{}/checkpoints/{}",
                TEST_BUCKET_NAME,
                test_case.dataset_id.as_multibase(),
                physical_hash.as_multibase()
            )
            .as_str() &&
            download_from_headers == vec![]
    );

    assert_matches!(
        push_strategy_existing_slice,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type: messages::ObjectType::DataSlice,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::SkipUpload,
            upload_to: None
        }
    );

    assert_matches!(
        push_strategy_existing_checkpoint,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type: messages::ObjectType::Checkpoint,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::SkipUpload,
            upload_to: None
        }
    );

    assert_matches!(
        push_strategy_new_slice,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::HttpUpload,
            upload_to: Some(messages::TransferUrl {
                url: upload_to_url,
                headers: upload_to_headers,
                ..
            })
        } if
            object_type == messages::ObjectType::DataSlice &&
            upload_to_url.path() == format!(
                "/{}/{}/data/{}",
                TEST_BUCKET_NAME,
                test_case.dataset_id.as_multibase(),
                physical_hash.as_multibase()
            ).as_str() &&
            upload_to_headers == vec![]
    );

    assert_matches!(
        push_strategy_new_checkpoint,
        messages::PushObjectTransferStrategy {
            object_file: messages::ObjectFileReference {
                object_type,
                physical_hash,
                ..
            },
            push_strategy: messages::ObjectPushStrategy::HttpUpload,
            upload_to: Some(messages::TransferUrl {
                url: upload_to_url,
                headers: upload_to_headers,
                ..
            })
        } if
            object_type == messages::ObjectType::Checkpoint &&
            upload_to_url.path() == format!(
                "/{}/{}/checkpoints/{}",
                TEST_BUCKET_NAME,
                test_case.dataset_id.as_multibase(),
                physical_hash.as_multibase()
            )
            .as_str() &&
            upload_to_headers == vec![]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestCase {
    pub dataset: Arc<dyn Dataset>,
    pub dataset_id: DatasetID,
    pub dataset_url: Url,
    pub object_file_references: Vec<messages::ObjectFileReference>,
    pub bearer_header: BearerHeader,
}

impl TestCase {
    pub fn data_slice_object(&self) -> &messages::ObjectFileReference {
        self.object_file_references.first().unwrap()
    }

    pub fn checkpoint_object(&self) -> &messages::ObjectFileReference {
        self.object_file_references.get(1).unwrap()
    }
}

async fn create_test_case(server_harness: &dyn ServerSideHarness) -> TestCase {
    let create_dataset_from_snapshot = server_harness.cli_create_dataset_from_snapshot_use_case();

    let create_result = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
            Default::default(),
        )
        .await
        .unwrap();

    let commit_result = commit_add_data_event(
        server_harness.cli_dataset_registry().as_ref(),
        &make_dataset_ref(None, "foo"),
        &server_harness.dataset_layout(&create_result.dataset_handle),
        None,
    )
    .await;

    let bearer_header = TypedHeader(
        headers::Authorization::<headers::authorization::Bearer>::bearer(DUMMY_ACCESS_TOKEN)
            .unwrap(),
    );

    let dataset_url = server_harness.dataset_url(&create_result.dataset_handle.alias);
    let object_file_references = collect_object_references_from_interval(
        create_result.dataset.as_ref(),
        &commit_result.new_head,
        None,
        false,
        false,
    )
    .await
    .unwrap();

    assert_eq!(2, object_file_references.len());

    TestCase {
        dataset: create_result.dataset.clone(),
        dataset_id: create_result.dataset_handle.id,
        dataset_url,
        bearer_header,
        object_file_references,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
