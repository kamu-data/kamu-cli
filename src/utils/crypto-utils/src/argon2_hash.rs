// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aes_gcm::aead::OsRng;
use argon2::PasswordHash;
use password_hash::{PasswordHasher, PasswordVerifier, SaltString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_argon2_hash(password: &str, hashing_mode: PasswordHashingMode) -> String {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = setup_argon2(hashing_mode);

    argon2
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn verify_argon2_hash(
    password: &str,
    password_hash: &str,
    hashing_mode: PasswordHashingMode,
) -> bool {
    let password_hash = PasswordHash::new(password_hash).unwrap();
    let argon2 = setup_argon2(hashing_mode);

    argon2
        .verify_password(password.as_bytes(), &password_hash)
        .is_ok()
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn setup_argon2<'a>(password_hashing_mode: PasswordHashingMode) -> argon2::Argon2<'a> {
    use argon2::*;
    match password_hashing_mode {
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
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum PasswordHashingMode {
    Default,
    Minimal,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
