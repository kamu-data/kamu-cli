// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{AccountConfig, AccountIdentityGenerator};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountIdentityGenerator)]
pub struct AccountIdentityGeneratorSeeded;

// TODO: tests
impl AccountIdentityGenerator for AccountIdentityGeneratorSeeded {
    fn generate_ed25519(
        &self,
        account_config: &AccountConfig,
    ) -> (odf::metadata::SigningKey, odf::AccountID) {
        use odf::metadata::PrivateKey;

        let account_name_bytes = account_config.account_name.as_bytes();

        let mut seed_buf = [0_u8; PrivateKey::SECRET_KEY_LENGTH];
        let copy_len = account_name_bytes.len().min(seed_buf.len());
        seed_buf[..copy_len].copy_from_slice(&account_name_bytes[..copy_len]);

        let private_key = PrivateKey::from_bytes(&seed_buf);
        let account_id = odf::AccountID::from_signing_key(&private_key);

        (private_key.into(), account_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
