// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreRegistry;
use kamu::*;
use s3_utils::S3Context;
use test_utils::LocalS3Server;
use url::Url;

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_auth_explicit_endpoint() {
    let s3 = LocalS3Server::new().await;
    let s3_ctx = S3Context::from_url(&s3.url).await;

    let store_url = Url::parse(&format!("s3://{}/", s3.bucket)).unwrap();
    let reg = ObjectStoreRegistryImpl::new(vec![Arc::new(ObjectStoreBuilderS3::new(s3_ctx, true))]);
    let store = reg.get_store(&store_url).unwrap();

    let path = object_store::path::Path::parse("asdf").unwrap();
    store
        .put(&path, object_store::PutPayload::from_static(b"test"))
        .await
        .unwrap();

    let store = reg.get_store(&store_url).unwrap();
    let res = store.get(&path).await.unwrap();
    let data = res.bytes().await.unwrap();
    assert_eq!(&data[..], b"test");
}
