// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use odf_metadata::*;
use url::Url;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_bytes(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    assert!(!repo.contains(&hash_foo).await.unwrap());
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)),);

    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_foo.clone(),
        },
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap()
    );
    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_bar.clone(),
        },
        repo.insert_bytes(b"bar", InsertOpts::default())
            .await
            .unwrap()
    );

    assert!(repo.contains(&hash_foo).await.unwrap());
    assert!(repo.contains(&hash_bar).await.unwrap());
    pretty_assertions::assert_eq!(b"foo", &repo.get_bytes(&hash_foo).await.unwrap()[..]);
    pretty_assertions::assert_eq!(b"bar", &repo.get_bytes(&hash_bar).await.unwrap()[..]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");

    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_foo.clone(),
        },
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap()
    );

    pretty_assertions::assert_eq!(b"foo", &repo.get_bytes(&hash_foo).await.unwrap()[..]);

    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_foo.clone(),
        },
        repo.insert_bytes(b"foo", InsertOpts::default())
            .await
            .unwrap()
    );

    pretty_assertions::assert_eq!(b"foo", &repo.get_bytes(&hash_foo).await.unwrap()[..]);

    repo.delete(&hash_foo).await.unwrap();

    assert_matches!(
        repo.get_bytes(&hash_foo).await,
        Err(GetError::NotFound(e)) if e.hash == hash_foo
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_precomputed(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_bar.clone(),
        },
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                precomputed_hash: Some(&hash_bar),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    );
    pretty_assertions::assert_eq!(b"foo", &repo.get_bytes(&hash_bar).await.unwrap()[..]);
    assert_matches!(repo.get_bytes(&hash_foo).await, Err(GetError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_insert_expect(repo: &dyn ObjectRepository) {
    let hash_foo = Multihash::from_digest_sha3_256(b"foo");
    let hash_bar = Multihash::from_digest_sha3_256(b"bar");

    pretty_assertions::assert_eq!(
        InsertResult {
            hash: hash_foo.clone(),
        },
        repo.insert_bytes(
            b"foo",
            InsertOpts {
                expected_hash: Some(&hash_foo),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    );
    pretty_assertions::assert_eq!(b"foo", &repo.get_bytes(&hash_foo).await.unwrap()[..]);

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExternalUrlTestOptions {
    pub cut_query_params: bool,
}

pub async fn test_external_urls(
    repo: &dyn ObjectRepository,
    hash: &Multihash,
    expected_external_download_url: Result<Url, GetExternalUrlError>,
    expected_external_upload_url_result: Result<Url, GetExternalUrlError>,
    opts: ExternalUrlTestOptions,
) {
    {
        let actual_external_download_url = repo
            .get_external_download_url(hash, ExternalTransferOpts { expiration: None })
            .await
            .map(|res| {
                let mut url = res.url;
                if opts.cut_query_params {
                    url.set_query(None);
                }
                url
            });

        pretty_assertions::assert_eq!(
            format!("{expected_external_download_url:?}"),
            format!("{actual_external_download_url:?}")
        );
    }
    {
        let actual_external_upload_url = repo
            .get_external_upload_url(hash, ExternalTransferOpts { expiration: None })
            .await
            .map(|res| {
                let mut url = res.url;
                if opts.cut_query_params {
                    url.set_query(None);
                }
                url
            });

        pretty_assertions::assert_eq!(
            format!("{expected_external_upload_url_result:?}"),
            format!("{actual_external_upload_url:?}")
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
