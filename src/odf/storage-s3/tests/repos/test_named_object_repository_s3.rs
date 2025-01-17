// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf_storage::testing::test_named_object_repository_shared;
use opendatafabric_storage_s3::*;
use test_utils::LocalS3Server;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_basics_s3() {
    let s3 = LocalS3Server::new().await;
    let s3_context = s3_utils::S3Context::from_url(&s3.url).await;
    let repo = NamedObjectRepositoryS3::new(s3_context);

    test_named_object_repository_shared::test_named_repository_operations(&repo).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
