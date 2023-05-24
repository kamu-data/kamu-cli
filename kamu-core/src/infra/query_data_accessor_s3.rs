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

use datafusion::prelude::SessionContext;
use dill::*;
use object_store::aws::AmazonS3Builder;
use opendatafabric::{DataSlice, DatasetHandle};
use url::Url;

use crate::domain::{InternalError, QueryDataAccessor, ResultIntoInternal};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct QueryDataAccessorS3 {
    bucket_name: String,
    endpoint: String,
    allow_http: bool,
}

impl QueryDataAccessorS3 {
    pub fn new(bucket_name: String, endpoint: String, allow_http: bool) -> Self {
        Self {
            bucket_name,
            endpoint,
            allow_http,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl QueryDataAccessor for QueryDataAccessorS3 {
    fn bind_object_store(&self, session_context: &SessionContext) -> Result<(), InternalError> {
        let s3_builder = AmazonS3Builder::new()
            .with_endpoint(self.endpoint.clone())
            .with_bucket_name(self.bucket_name.clone())
            .with_region(env::var("AWS_DEFAULT_REGION").unwrap())
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
            .with_allow_http(self.allow_http);

        let s3 = s3_builder.build().int_err()?;

        let s3_url = Url::parse(self.endpoint.as_str()).unwrap();
        session_context
            .runtime_env()
            .register_object_store(&s3_url, Arc::new(s3));

        Ok(())
    }

    fn object_store_url(&self) -> Url {
        Url::parse(&self.endpoint.as_str()).unwrap()
    }

    fn data_object_store_path(
        &self,
        dataset_handle: &DatasetHandle,
        data_slice: &DataSlice,
    ) -> object_store::path::Path {
        assert!(
            !dataset_handle.alias.is_multitenant(),
            "Multitenancy is not yet supported"
        );

        let slice_path_str = format!(
            "{}/data/{}",
            dataset_handle.alias.dataset_name.as_str(),
            data_slice.physical_hash.to_multibase_string()
        );
        object_store::path::Path::from_url_path(slice_path_str).unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
