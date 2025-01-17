// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

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
