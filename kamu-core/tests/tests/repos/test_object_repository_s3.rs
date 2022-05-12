// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::*;
use opendatafabric::*;

use std::assert_matches::assert_matches;
use url::Url;

use crate::utils::MinioServer;

#[allow(dead_code)]
struct S3 {
    tmp_dir: tempfile::TempDir,
    minio: MinioServer,
    url: Url,
}

fn run_s3_server() -> S3 {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_dir.path(), access_key, secret_key);

    let url = Url::parse(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    S3 {
        tmp_dir,
        minio,
        url,
    }
}

#[ignore = "Rusoto does not handle 403 correctly"]
#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_unauthorized() {
    let s3 = run_s3_server();
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "BAD_KEY");
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    assert_matches!(
        repo.insert_bytes(b"foo", InsertOpts::default()).await,
        Err(InsertError::Access(AccessError::Unauthorized(_)))
    );
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_bytes() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert!(!repo.contains(&hash_foo).await.unwrap());
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)),);

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );
    assert_eq!(
        repo.insert_bytes(b"bar", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_bar.clone(),
            already_existed: false
        }
    );

    assert!(repo.contains(&hash_foo).await.unwrap());
    assert!(repo.contains(&hash_bar).await.unwrap());
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"bar");
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_bytes_long() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    use rand::RngCore;

    let mut data = [0u8; 16000];
    rand::thread_rng().fill_bytes(&mut data);

    let hash = Multihash::from_digest_sha3_256(&data);

    assert_eq!(
        repo.insert_bytes(&data, InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash.clone(),
            already_existed: false,
        }
    );

    assert_eq!(&repo.get_bytes(&hash).await.unwrap()[..], data);
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_stream() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

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
            already_existed: false
        }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash_foobar).await.unwrap();
    let mut data = Vec::new();
    stream.read_to_end(&mut data).await.unwrap();

    assert_eq!(data, b"foobar");
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_stream_long() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    use rand::RngCore;

    let mut data = [0u8; 16000];
    rand::thread_rng().fill_bytes(&mut data);
    let hash = Multihash::from_digest_sha3_256(&data);

    assert_eq!(
        repo.insert_stream(
            Box::new(std::io::Cursor::new(Vec::from(data))),
            InsertOpts {
                precomputed_hash: Some(&hash),
                size_hint: Some(data.len()),
                ..Default::default()
            }
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash.clone(),
            already_existed: false
        }
    );

    use tokio::io::AsyncReadExt;
    let mut stream = repo.get_stream(&hash).await.unwrap();
    let mut data_received = Vec::new();
    stream.read_to_end(&mut data_received).await.unwrap();

    assert_eq!(data, data_received[..]);
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_delete() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    assert_eq!(
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false // Note: S3's insert_bytes does not check for existence to save a roundtrip
        }
    );

    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    repo.delete(&hash_foo).await.unwrap();

    assert_matches!(
        repo.get_bytes(&hash_foo).await,
        Err(GetError::NotFound(e)) if e.hash == hash_foo
    );
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_precomputed() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert_eq!(
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                precomputed_hash: Some(&hash_bar),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash_bar.clone(),
            already_existed: false
        }
    );
    assert_eq!(&repo.get_bytes(&hash_bar).await.unwrap()[..], b"foo");
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)));
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_insert_expect() {
    let s3 = run_s3_server();
    let repo = ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::from_url(&s3.url);

    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert_eq!(
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                expected_hash: Some(&hash_foo),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
        InsertResult {
            hash: hash_foo.clone(),
            already_existed: false
        }
    );
    assert_eq!(&repo.get_bytes(&hash_foo).await.unwrap()[..], b"foo");

    assert_matches!(
        repo.insert_bytes(
            b"bar",
            InsertOpts {
                expected_hash: Some(&hash_foo),
                ..Default::default()
            },
        )
        .await,
        Err(InsertError::HashMismatch(HashMismatchError {
            expected,
            actual,
        })) if expected == hash_foo && actual == hash_bar
    );
    assert_matches!(repo.get_bytes(&hash_bar).await, Err(GetError::NotFound(_)));
}
