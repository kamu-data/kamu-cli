// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf_storage::testing::test_named_object_repository_shared;
use opendatafabric_storage_lfs::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_basics_local_fs() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let repo = NamedObjectRepositoryLocalFS::new(tmp_dir.path());

    test_named_object_repository_shared::test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
