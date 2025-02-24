// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use aws_credential_types::provider::SharedCredentialsProvider;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::CredentialProvider;
use s3_utils::S3Context;
use url::Url;

use super::object_store_with_tracing::ObjectStoreWithTracing;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ObjectStoreBuilderS3
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct ObjectStoreBuilderS3 {
    s3_context: S3Context,
    allow_http: bool,
}

impl ObjectStoreBuilderS3 {
    pub fn new(s3_context: S3Context, allow_http: bool) -> Self {
        Self {
            s3_context,
            allow_http,
        }
    }
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
            .with_credentials(Arc::new(AwsSdkCredentialProvider::new(
                self.s3_context
                    .credentials_provider()
                    .expect("No credentials provider"),
            )))
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
// AwsSdkCredentialProvider
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The [`object_store`] crate doesn't use the official AWS SDK and has its
/// own credential resolution mechanisms. We want to avoid using two paths to
/// source credentials as they sometimes don't agree with one another. We also
/// want to reuse credentials between the two subsystems. This type
/// implements a bridge between the credentials cache in SDK and credentials
/// provider in [`object_store`] crate.
#[derive(Debug)]
struct AwsSdkCredentialProvider {
    credentials_provider: SharedCredentialsProvider,
    state: Mutex<AwsSdkCredentialProviderState>,
}

#[derive(Debug, Default)]
struct AwsSdkCredentialProviderState {
    cached_creds: Option<aws_credential_types::Credentials>,
    converted_creds: Option<Arc<AwsCredential>>,
}

impl AwsSdkCredentialProvider {
    fn new(credentials_provider: SharedCredentialsProvider) -> Self {
        Self {
            credentials_provider,
            state: Mutex::new(AwsSdkCredentialProviderState::default()),
        }
    }
}

#[async_trait::async_trait]
impl CredentialProvider for AwsSdkCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<Self::Credential>, object_store::Error> {
        use aws_credential_types::provider::ProvideCredentials;
        let new_creds = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: e.into(),
            })?;

        let mut state = self.state.lock().unwrap();

        // Are cached credentials returned by SDK still valid?
        if let Some(cached_creds) = &state.cached_creds {
            if *cached_creds == new_creds {
                // Yes - reuse last converted result
                return Ok(Arc::clone(state.converted_creds.as_ref().unwrap()));
            }
        }

        // Convert and cache the credentials
        let converted_creds = Arc::new(AwsCredential {
            key_id: new_creds.access_key_id().to_string(),
            secret_key: new_creds.secret_access_key().to_string(),
            token: new_creds.session_token().map(ToString::to_string),
        });

        state.cached_creds = Some(new_creds);
        state.converted_creds = Some(Arc::clone(&converted_creds));

        Ok(converted_creds)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
