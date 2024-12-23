// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use odf_storage::testing::test_object_repository_shared;
use odf_storage::{ObjectRepository, ObjectRepositoryProtocol};
use opendatafabric_storage_inmem::ObjectRepositoryInMemory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_protocol() {
    let repo = ObjectRepositoryInMemory::new();
    assert_matches!(repo.protocol(), ObjectRepositoryProtocol::Memory);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_bytes() {
    let repo = ObjectRepositoryInMemory::new();
    test_object_repository_shared::test_insert_bytes(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete() {
    let repo = ObjectRepositoryInMemory::new();
    test_object_repository_shared::test_delete(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_precomputed() {
    let repo = ObjectRepositoryInMemory::new();
    test_object_repository_shared::test_insert_precomputed(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_expect() {
    let repo = ObjectRepositoryInMemory::new();
    test_object_repository_shared::test_insert_expect(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
