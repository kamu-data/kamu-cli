// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::AccountName;

use crate::auth;

/////////////////////////////////////////////////////////////////////////////////////////

pub const TEST_ACCOUNT_NAME: &str = "kamu";

/////////////////////////////////////////////////////////////////////////////////////////

pub struct CurrentAccountSubject {
    pub account: auth::AccountInfo,
}

impl CurrentAccountSubject {
    pub fn new_test() -> Self {
        CurrentAccountSubject::new(auth::AccountInfo {
            name: TEST_ACCOUNT_NAME.to_string(),
            login: AccountName::new_unchecked(TEST_ACCOUNT_NAME),
            avatar_url: None,
        })
    }

    pub fn new(account: auth::AccountInfo) -> Self {
        Self { account }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
