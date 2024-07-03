// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::provider::ProvideCredentials;
use aws_runtime::auth::sigv4::SigV4Signer;
use aws_runtime::auth::{HttpSignatureType, SigV4OperationSigningConfig, SigningOptions};
use aws_smithy_async::time::SystemTimeSource;
use aws_smithy_runtime_api::client::auth::{AuthSchemeEndpointConfig, Sign};
use aws_smithy_runtime_api::client::identity::Identity;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::{ConfigBag, Layer};
use aws_types::region::SigningRegion;
use aws_types::sdk_config::SharedTimeSource;
use aws_types::SigningName;
use dill::*;
use http::Request;
use internal_error::InternalError;
use secrecy::{ExposeSecret, Secret};

use crate::{DatabaseConnectionSettings, DatabaseCredentials, DatabasePasswordProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseAwsIamTokenProvider {
    db_user_name: Secret<String>,
    db_connection_settings: DatabaseConnectionSettings,
}

impl DatabaseAwsIamTokenProvider {
    pub fn new(
        db_user_name: Secret<String>,
        db_connection_settings: DatabaseConnectionSettings,
    ) -> Self {
        Self {
            db_user_name,
            db_connection_settings,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseAwsIamTokenProvider {
    async fn provide_credentials(&self) -> Result<Option<DatabaseCredentials>, InternalError> {
        // Inspired by https://gist.github.com/ysaito1001/6619bf34f2c53d81d37cdd58515092ce
        let region_provider = RegionProviderChain::default_provider().or_else("unspefified");
        let config = aws_config::from_env().region(region_provider).load().await;

        let credentials = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        let region = config.region().unwrap();

        let mut request = Request::builder()
            .uri(format!(
                "http://{db_hostname}:{port}/?Action=connect&DBUser={db_user}",
                db_hostname = self.db_connection_settings.host,
                port = self.db_connection_settings.port(),
                db_user = self.db_user_name.expose_secret()
            ))
            .body(SdkBody::empty())
            .unwrap()
            .try_into()
            .unwrap();

        let identity = Identity::new(credentials, None);

        let mut signing_options = SigningOptions::default();
        signing_options.signature_type = HttpSignatureType::HttpRequestQueryParams;
        signing_options.expires_in = Some(Duration::from_secs(15 * 60));
        let signing_config = SigV4OperationSigningConfig {
            region: Some(SigningRegion::from(region.clone())),
            name: Some(SigningName::from_static("rds-db")),
            signing_options,
            ..Default::default()
        };

        let time_source = SharedTimeSource::new(SystemTimeSource::new());
        let mut rc_builder = RuntimeComponentsBuilder::for_tests();
        rc_builder.set_time_source(Some(time_source));
        let runtime_components = rc_builder.build().unwrap();

        let mut layer = Layer::new("SigningConfig");
        layer.store_put(signing_config);
        let config_bag = ConfigBag::of_layers(vec![layer]);

        let signer = SigV4Signer::new();
        let _ = signer.sign_http_request(
            &mut request,
            &identity,
            AuthSchemeEndpointConfig::empty(),
            &runtime_components,
            &config_bag,
        );

        let mut uri = request.uri().to_string();
        assert!(uri.starts_with("http://"));
        let token = uri.split_off("http://".len());

        Ok(Some(DatabaseCredentials {
            user_name: self.db_user_name.clone(),
            password: Secret::new(token),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
