// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::repos::named_object_repository::GetError;
use kamu::domain::*;
use kamu::infra::*;

use std::assert_matches::assert_matches;

#[tokio::test]
async fn test_basics() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));

    repo.set("head", b"foo").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"foo");

    repo.set("head", b"bar").await.unwrap();
    assert_eq!(&repo.get("head").await.unwrap()[..], b"bar");

    repo.delete("head").await.unwrap();
    assert_matches!(repo.get("head").await, Err(GetError::NotFound(_)));
}
