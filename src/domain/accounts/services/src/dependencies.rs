// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(b: &mut CatalogBuilder, needs_indexing: bool, production: bool) {
    b.add::<AccessTokenServiceImpl>();
    b.add::<AccountServiceImpl>();
    b.add::<AuthenticationServiceImpl>();
    b.add::<LoginPasswordAuthProvider>();
    b.add::<PredefinedAccountsRegistrator>();

    b.add::<CreateAccountUseCaseImpl>();
    b.add::<DeleteAccountUseCaseImpl>();
    b.add::<UpdateAccountUseCaseImpl>();
    b.add::<ModifyAccountPasswordUseCaseImpl>();

    b.add::<utils::AccountAuthorizationHelperImpl>();

    b.add::<DidSecretService>();

    if needs_indexing {
        b.add::<OAuthDeviceCodeServiceImpl>();

        if production {
            b.add::<OAuthDeviceCodeGeneratorDefault>();
        } else {
            b.add::<PredefinedOAuthDeviceCodeGenerator>();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
