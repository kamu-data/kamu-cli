// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use object_store::aws::AmazonS3Builder;
use s3_utils::S3Context;
use url::Url;

use super::object_store_with_tracing::ObjectStoreWithTracing;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ObjectStoreBuilderS3
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub struct ObjectStoreBuilderS3 {
    #[component(explicit)]
    s3_context: S3Context,

    #[component(explicit)]
    allow_http: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl ObjectStoreBuilder for ObjectStoreBuilderS3 {
    fn object_store_url(&self) -> Url {
        // TODO: This URL does not account for endpoint and it will collide in case we
        // work with multiple S3-like storages having same buckets names
        Url::parse(format!("s3://{}/", self.s3_context.bucket()).as_str()).unwrap()
    }

    #[tracing::instrument(level = "info", name = ObjectStoreBuilderS3_build_object_store, skip_all)]
    fn build_object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>, InternalError> {
        tracing::info!(
            endpoint = self.s3_context.endpoint(),
            region = self.s3_context.region(),
            bucket = self.s3_context.bucket(),
            allow_http = %self.allow_http,
            "Building object store",
        );

        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(self.s3_context.bucket())
            .with_allow_http(self.allow_http);

        if let Some(endpoint) = self.s3_context.endpoint() {
            s3_builder = s3_builder.with_endpoint(endpoint);
        }

        if let Some(region) = self.s3_context.region() {
            s3_builder = s3_builder.with_region(region);
        }

        let object_store = s3_builder
            .build()
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Failed to build S3 object store");
                e
            })
            .int_err()?;

        Ok(Arc::new(ObjectStoreWithTracing::new(object_store)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
