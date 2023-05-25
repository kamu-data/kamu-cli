// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::sync::Arc;

use dill::*;
use object_store::aws::AmazonS3Builder;
use url::Url;

use super::LogicalUrlConfig;
use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct ObjectStoreBuilderS3 {
    bucket_name: String,
    endpoint: String,
    allow_http: bool,
    logical_url_config: LogicalUrlConfig,
}

impl ObjectStoreBuilderS3 {
    pub fn new(
        bucket_name: String,
        endpoint: String,
        allow_http: bool,
        logical_url_config: LogicalUrlConfig,
    ) -> Self {
        Self {
            bucket_name,
            endpoint,
            allow_http,
            logical_url_config,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl ObjectStoreBuilder for ObjectStoreBuilderS3 {
    fn object_store_url(&self) -> Url {
        // TODO: This URL does not account for endpoint and it will collide in case we
        // work with multiple S3-like storages having same buckets names
        self.logical_url_config.logical_root.clone()
    }

    fn build_object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>, InternalError> {
        let s3_builder = AmazonS3Builder::new()
            .with_endpoint(self.endpoint.clone())
            .with_bucket_name(self.bucket_name.clone())
            .with_region(env::var("AWS_DEFAULT_REGION").unwrap())
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
            .with_allow_http(self.allow_http);

        let object_store = s3_builder.build().int_err()?;

        Ok(Arc::new(object_store))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
