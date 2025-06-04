// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::provider::ProvideCredentials;
use chrono::Utc;
use dill::*;
use hmac::{Hmac, Mac};
use internal_error::{InternalError, ResultIntoInternal};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};

use crate::{DatabaseConnectionSettings, DatabaseCredentials, DatabasePasswordProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseAwsIamTokenProvider {
    db_connection_settings: DatabaseConnectionSettings,

    #[component(explicit)]
    db_user_name: SecretString,
}

impl DatabaseAwsIamTokenProvider {
    fn get_signature_key(
        key: &str,
        date_stamp: &str,
        region_name: &str,
        service_name: &str,
    ) -> Vec<u8> {
        let k_date = Self::hmac_sha256(format!("AWS4{key}").as_bytes(), date_stamp.as_bytes());
        let k_region = Self::hmac_sha256(&k_date, region_name.as_bytes());
        let k_service = Self::hmac_sha256(&k_region, service_name.as_bytes());
        Self::hmac_sha256(&k_service, b"aws4_request")
    }

    fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.finalize().into_bytes().to_vec()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseAwsIamTokenProvider {
    async fn provide_credentials(&self) -> Result<Option<DatabaseCredentials>, InternalError> {
        let region_provider = RegionProviderChain::default_provider().or_else("unspefified");
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let creds = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        let access_key = creds.access_key_id();
        let secret_key = creds.secret_access_key();
        let session_token = creds.session_token().unwrap_or_default();

        let endpoint = format!(
            "{}:{}/{}",
            self.db_connection_settings.host,
            self.db_connection_settings.port(),
            self.db_user_name.expose_secret(),
        );
        let canonical_request = format!(
            "GET\n{}\n\nhost:{}\n\nhost\n",
            endpoint, self.db_connection_settings.host,
        );

        let date = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let credential_scope = format!("{}/{}/rds-db/aws4_request", date, "us-west-2");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date,
            credential_scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        let signing_key = Self::get_signature_key(secret_key, &date, "us-west-2", "rds-db");
        let signature = hex::encode(
            Hmac::<Sha256>::new_from_slice(&signing_key)
                .int_err()?
                .chain_update(string_to_sign.as_bytes())
                .finalize()
                .into_bytes(),
        );

        let token = format!(
            "https://{endpoint}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={access_key}%2F{credential_scope}/rds-db/aws4_request&X-Amz-Date={date}&X-Amz-SignedHeaders=host&X-Amz-Signature={signature}&X-Amz-Security-Token={session_token}",
        );

        Ok(Some(DatabaseCredentials {
            user_name: self.db_user_name.clone(),
            password: SecretString::from(token),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
