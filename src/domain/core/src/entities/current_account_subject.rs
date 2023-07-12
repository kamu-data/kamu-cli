// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::AccountName;

/////////////////////////////////////////////////////////////////////////////////////////

pub const TEST_ACCOUNT_NAME: &str = "kamu";

/////////////////////////////////////////////////////////////////////////////////////////

pub struct CurrentAccountSubject {
    pub account_name: AccountName,
}

impl CurrentAccountSubject {
    pub fn new_test() -> Self {
        CurrentAccountSubject::new(TEST_ACCOUNT_NAME)
    }

    pub fn new<S>(account_name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            account_name: AccountName::try_from(account_name.into()).unwrap(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
