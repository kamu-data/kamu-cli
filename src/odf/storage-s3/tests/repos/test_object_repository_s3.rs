// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::convert::TryFrom;

use odf_metadata::*;
use odf_storage::testing::test_object_repository_shared;
use odf_storage::testing::test_object_repository_shared::ExternalUrlTestOptions;
use odf_storage::*;
use opendatafabric_storage_s3::*;
use s3_utils::S3Context;
use test_utils::{LocalS3Server, TEST_BUCKET_NAME};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_protocol() {
    let s3 = LocalS3Server::new().await;
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "BAD_KEY");
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    assert_matches!(repo.protocol(), ObjectRepositoryProtocol::S3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[ignore = "We do not yet handle unauthorized errors correctly"]
#[test_log::test(tokio::test)]
async fn test_unauthorized() {
    let s3 = LocalS3Server::new().await;
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "BAD_KEY");
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    assert_matches!(
        repo.insert_bytes(b"foo", InsertOpts::default()).await,
        Err(InsertError::Access(AccessError::Unauthorized(_)))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_bytes() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    test_object_repository_shared::test_insert_bytes(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_bytes_long() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    use rand::RngCore;

    let mut data = [0u8; 16000];
    rand::thread_rng().fill_bytes(&mut data);

    let hash = Multihash::from_digest_sha3_256(&data);

    assert_eq!(
        repo.insert_bytes(&data, InsertOpts::default())
            .await
            .unwrap(),
        InsertResult { hash: hash.clone() }
    );

    assert_eq!(&repo.get_bytes(&hash).await.unwrap()[..], data);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_stream() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    let hash_foobar = Multihash::from_digest_sha3_256(b"foobar");

    assert_matches!(
        repo.get_stream(&hash_foobar).await.err().unwrap(),
        GetError::NotFound(_),
    );

    assert_eq!(
        repo.insert_stream(
            Box::new(std::io::Cursor::new(b"foobar")),
            InsertOpts {
                precomputed_hash: Some(&hash_foobar),
                size_hint: Some(6),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash_foobar.clone(),
        }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash_foobar).await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();

    assert_eq!(data, b"foobar");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_stream_long() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    use rand::RngCore;

    let mut data = [0u8; 16000];
    rand::thread_rng().fill_bytes(&mut data);
    let hash = Multihash::from_digest_sha3_256(&data);

    assert_eq!(
        repo.insert_stream(
            Box::new(std::io::Cursor::new(Vec::from(data))),
            InsertOpts {
                precomputed_hash: Some(&hash),
                size_hint: Some(u64::try_from(data.len()).unwrap()),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        InsertResult { hash: hash.clone() }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash).await.unwrap();
    let mut data_received = Vec::new();
    stream.read_to_end(&mut data_received).await.unwrap();

    assert_eq!(data, data_received[..]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_delete() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    test_object_repository_shared::test_delete(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_precomputed() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    test_object_repository_shared::test_insert_precomputed(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_insert_expect() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    test_object_repository_shared::test_insert_expect(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_external_urls() {
    let s3 = LocalS3Server::new().await;
    let repo = ObjectRepositoryS3Sha3::new(S3Context::from_url(&s3.url).await);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");

    let expected_external_download_url = {
        let mut url =
            Url::parse(&format!("http://127.0.0.1/{TEST_BUCKET_NAME}/{hash_foo}")).unwrap();
        url.set_port(s3.url.port()).unwrap();
        url
    };
    let expected_external_upload_url_result = expected_external_download_url.clone();

    test_object_repository_shared::test_external_urls(
        &repo,
        &hash_foo,
        Ok(expected_external_download_url),
        Ok(expected_external_upload_url_result),
        ExternalUrlTestOptions {
            cut_query_params: true,
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
