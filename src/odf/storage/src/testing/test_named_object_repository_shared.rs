// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_named_repository_operations(repo: &dyn NamedObjectRepository) {
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));

    repo.set("head", b"foo").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    repo.set("head", b"bar").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    repo.delete("head").await.unwrap();
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_named_repository_compare_and_set(repo: &dyn NamedObjectRepository) {
    // Expecting content of an absent object
    assert_matches!(
        repo.compare_and_set("head", Some(b"foo"), b"bar").await,
        Err(CompareAndSetNamedError::Mismatch(_))
    );
    assert_matches!(repo.get("head").await, Err(GetNamedError::NotFound(_)));

    // Expecting absence
    repo.compare_and_set("head", None, b"foo").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    assert_matches!(
        repo.compare_and_set("head", None, b"bar").await,
        Err(CompareAndSetNamedError::Mismatch(_))
    );
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    // Expecting the current content
    repo.compare_and_set("head", Some(b"foo"), b"bar")
        .await
        .unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    // Expecting stale content
    assert_matches!(
        repo.compare_and_set("head", Some(b"foo"), b"baz").await,
        Err(CompareAndSetNamedError::Mismatch(_))
    );
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    repo.delete("head").await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Writers that all observed the same content race to replace it: exactly one
/// wins, and the object holds its content
pub async fn test_named_repository_compare_and_set_race(repo: &dyn NamedObjectRepository) {
    const WRITERS: usize = 8;

    repo.set("head", b"base").await.unwrap();

    let contents: Vec<_> = (0..WRITERS).map(|i| format!("writer-{i}")).collect();
    let results = futures::future::join_all(
        contents
            .iter()
            .map(|content| repo.compare_and_set("head", Some(b"base"), content.as_bytes())),
    )
    .await;

    let winners: Vec<_> = results
        .iter()
        .zip(&contents)
        .filter(|(result, _)| result.is_ok())
        .map(|(_, content)| content.as_bytes())
        .collect();
    assert_eq!(winners.len(), 1, "{results:?}");
    for result in &results {
        if let Err(err) = result {
            assert_matches!(err, CompareAndSetNamedError::Mismatch(_));
        }
    }
    assert_eq!(&repo.get("head").await.unwrap()[..], winners[0]);

    repo.delete("head").await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
