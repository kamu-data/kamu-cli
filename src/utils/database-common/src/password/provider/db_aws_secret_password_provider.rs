// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_secretsmanager::Client;
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;

use crate::{DatabaseCredentials, DatabasePasswordProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseAwsSecretPasswordProvider {
    #[component(explicit)]
    secret_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseAwsSecretPasswordProvider {
    async fn provide_credentials(&self) -> Result<Option<DatabaseCredentials>, InternalError> {
        let region_provider = RegionProviderChain::default_provider().or_else("unspecified");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);

        let response = client
            .get_secret_value()
            .secret_id(&self.secret_name)
            .send()
            .await
            .int_err()?;

        match response.secret_string() {
            Some(secret_string) => {
                let secret_value =
                    serde_json::from_str::<AwsRdsSecretValue>(secret_string).int_err()?;
                Ok(Some(DatabaseCredentials {
                    user_name: SecretString::from(secret_value.username.clone()),
                    password: SecretString::from(secret_value.password.clone()),
                }))
            }
            None => Err(AwsSecretNotFoundError {
                secret_name: self.secret_name.clone(),
            }
            .int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("AWS secret {secret_name} not found")]
struct AwsSecretNotFoundError {
    pub secret_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This matches JSON structure stored in AWS Secrets Manager for secrets
/// generated via auto-managed database passwords
#[derive(Debug, Deserialize)]
struct AwsRdsSecretValue {
    pub username: String,
    pub password: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
