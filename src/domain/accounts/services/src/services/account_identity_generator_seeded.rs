// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::AccountIdentityGenerator;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountIdentityGenerator)]
pub struct AccountIdentityGeneratorSeeded;

impl AccountIdentityGenerator for AccountIdentityGeneratorSeeded {
    fn generate_ed25519(
        &self,
        account_name: &odf::AccountName,
    ) -> (odf::metadata::SigningKey, odf::AccountID) {
        use odf::metadata::PrivateKey;

        let private_key = PrivateKey::from_bytes_padded(account_name.as_bytes());
        let account_id = odf::AccountID::from_signing_key(&private_key);

        (private_key.into(), account_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
