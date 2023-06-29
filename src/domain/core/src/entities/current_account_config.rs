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

#[derive(Debug, Clone)]
pub struct CurrentAccountConfig {
    pub account_name: AccountName,
    pub specified_explicitly: bool,
}

impl CurrentAccountConfig {
    pub fn new<S>(account_name: S, specified_explicitly: bool) -> Self
    where
        S: Into<String>,
    {
        Self {
            account_name: AccountName::try_from(account_name.into()).unwrap(),
            specified_explicitly,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
