// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core as domain;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub struct Auth;

#[Object]
impl Auth {
    async fn enabled_login_methods(&self, ctx: &Context<'_>) -> Result<Vec<&'static str>> {
        let authentication_service =
            from_catalog::<dyn domain::auth::AuthenticationService>(ctx).unwrap();

        Ok(authentication_service.supported_login_methods())
    }
}

///////////////////////////////////////////////////////////////////////////////
