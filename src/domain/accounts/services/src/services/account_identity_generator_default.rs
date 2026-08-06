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
pub struct AccountIdentityGeneratorDefault;

impl AccountIdentityGenerator for AccountIdentityGeneratorDefault {
    fn generate_ed25519(
        &self,
        _account_name: &odf::AccountName,
    ) -> (odf::metadata::SigningKey, odf::AccountID) {
        odf::AccountID::new_generated_ed25519()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
