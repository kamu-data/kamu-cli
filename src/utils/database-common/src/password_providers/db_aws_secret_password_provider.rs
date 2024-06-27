// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_secretsmanager::Client;
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use secrecy::Secret;
use thiserror::Error;

use crate::DatabasePasswordProvider;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseAwsSecretPasswordProvider {
    secret_name: String,
}

impl DatabaseAwsSecretPasswordProvider {
    pub fn new(secret_name: String) -> Self {
        Self { secret_name }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseAwsSecretPasswordProvider {
    async fn provide_password(&self) -> Result<Option<Secret<String>>, InternalError> {
        let region_provider = RegionProviderChain::default_provider().or_else("unspecified");
        let config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&config);

        let response = client
            .get_secret_value()
            .secret_id(&self.secret_name)
            .send()
            .await
            .int_err()?;

        match response.secret_string() {
            Some(secret_string) => Ok(Some(Secret::new(secret_string.to_string()))),
            None => Err(AwsSecretNotFoundError {
                secret_name: self.secret_name.clone(),
            }
            .int_err()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("AWS secret {secret_name} not found")]
struct AwsSecretNotFoundError {
    pub secret_name: String,
}

/////////////////////////////////////////////////////////////////////////////////////////
