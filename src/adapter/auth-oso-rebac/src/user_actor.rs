// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;
use oso::PolarClass;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(PolarClass, Debug, Clone)]
pub struct UserActor {
    #[polar(attribute)]
    pub account_id: String,
    #[polar(attribute)]
    pub anonymous: bool,
    #[polar(attribute)]
    pub is_admin: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl UserActor {
    pub fn anonymous() -> Self {
        UserActor {
            account_id: String::new(),
            anonymous: true,
            is_admin: false,
        }
    }

    pub fn logged(account_id: &odf::AccountID, is_admin: bool) -> Self {
        Self {
            account_id: account_id.to_string(),
            anonymous: false,
            is_admin,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for UserActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "User(account_id={}, anonymous={}, is_admin={})",
            &self.account_id, self.anonymous, self.is_admin
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
