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

pub fn register_dependencies(
    catalog_builder: &mut CatalogBuilder,
    needs_indexing: bool,
    production: bool,
) {
    catalog_builder.add::<AccessTokenServiceImpl>();
    catalog_builder.add::<AccountServiceImpl>();
    catalog_builder.add::<AuthenticationServiceImpl>();
    catalog_builder.add::<LoginPasswordAuthProvider>();
    catalog_builder.add::<PredefinedAccountsRegistrator>();

    catalog_builder.add::<CreateAccountUseCaseImpl>();
    catalog_builder.add::<DeleteAccountUseCaseImpl>();

    if needs_indexing {
        catalog_builder.add::<OAuthDeviceCodeServiceImpl>();

        if production {
            catalog_builder.add::<OAuthDeviceCodeGeneratorDefault>();
        } else {
            catalog_builder.add::<PredefinedOAuthDeviceCodeGenerator>();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
