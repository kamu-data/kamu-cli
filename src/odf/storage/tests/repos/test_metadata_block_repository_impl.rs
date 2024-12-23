// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf_storage_lfs::ObjectRepositoryLocalFSSha3;
use opendatafabric_storage::MetadataBlockRepositoryImpl;

use super::test_metadata_block_repository_shared;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_insert_block() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let obj_repo = ObjectRepositoryLocalFSSha3::new(tmp_repo_dir.path());
    let repo = MetadataBlockRepositoryImpl::new(obj_repo);

    test_metadata_block_repository_shared::test_insert_block(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
