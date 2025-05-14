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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Hasher for Argon2Hasher<'_> {
    fn hash(&self, value: &[u8]) -> String {
        let salt = SaltString::generate(&mut OsRng);

        self.argon2.hash_password(value, &salt).unwrap().to_string()
    }

    fn verify(&self, value: &[u8], hashed_value: &str) -> bool {
        let password_hash = PasswordHash::new(hashed_value).unwrap();

        self.argon2.verify_password(value, &password_hash).is_ok()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum PasswordHashingMode {
    Default,
    Minimal,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
