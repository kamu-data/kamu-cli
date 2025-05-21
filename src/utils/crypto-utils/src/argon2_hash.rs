// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aes_gcm::aead::OsRng;
use argon2::{Algorithm, Argon2, Params, PasswordHash, Version};
use internal_error::{InternalError, ResultIntoInternal};
use password_hash::{PasswordHasher, PasswordVerifier, SaltString};

use crate::Hasher;

pub struct Argon2Hasher<'a> {
    argon2: Argon2<'a>,
}

impl Argon2Hasher<'_> {
    pub fn new(password_hashing_mode: PasswordHashingMode) -> Self {
        let argon2 = match password_hashing_mode {
            // Use default Argon2 settings in production
            PasswordHashingMode::Default => Argon2::default(),

            // Use minimal Argon2 settings in test mode
            PasswordHashingMode::Minimal => Argon2::new(
                Algorithm::default(),
                Version::default(),
                Params::new(
                    Params::MIN_M_COST,
                    Params::MIN_T_COST,
                    Params::MIN_P_COST,
                    None,
                )
                .expect("Settings for testing hashing mode must be fine"),
            ),
        };

        Self { argon2 }
    }

    fn hash_impl(&self, value: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);

        self.argon2.hash_password(value, &salt).unwrap().to_string()
    }

    fn verify_impl(&self, value: &[u8], hashed_value: &str) -> bool {
        let password_hash = PasswordHash::new(hashed_value).unwrap();

        self.argon2.verify_password(value, &password_hash).is_ok()
    }

    pub async fn hash_async(
        value: &[u8],
        password_hashing_mode: PasswordHashingMode,
    ) -> Result<String, InternalError> {
        let value = value.to_owned();

        // Generate hash: this is a compute-intensive operation,
        // so spawn a blocking task
        tokio::task::spawn_blocking(move || {
            tracing::info_span!("Generating hash").in_scope(|| {
                let argon2 = Self::new(password_hashing_mode);
                argon2.hash_impl(&value)
            })
        })
        .await
        .int_err()
    }

    pub async fn verify_async(
        value: &[u8],
        hashed_value: &str,
        password_hashing_mode: PasswordHashingMode,
    ) -> Result<bool, InternalError> {
        let value = value.to_owned();
        let hashed_value = hashed_value.to_owned();

        // Verify hash: this is a compute-intensive operation,
        // so spawn a blocking task
        tokio::task::spawn_blocking(move || {
            tracing::info_span!("Verifying hash").in_scope(|| {
                let argon2 = Self::new(password_hashing_mode);
                argon2.verify_impl(&value, &hashed_value)
            })
        })
        .await
        .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Hasher for Argon2Hasher<'_> {
    fn hash(&self, value: &[u8]) -> String {
        self.hash_impl(value)
    }

    fn verify(&self, value: &[u8], hashed_value: &str) -> bool {
        self.verify_impl(value, hashed_value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum PasswordHashingMode {
    Default,
    Minimal,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
