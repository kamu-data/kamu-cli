// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{AnonymousAccountReason, CurrentAccountSubject};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CurrentAccountSubjectTestHelper {}

impl CurrentAccountSubjectTestHelper {
    pub fn logged(account_name: &str) -> CurrentAccountSubject {
        let account_name = odf::AccountName::new_unchecked(account_name);
        let account_id = odf::AccountID::new_seeded_ed25519(account_name.as_bytes());
        let is_admin = false;

        CurrentAccountSubject::logged(account_id, account_name, is_admin)
    }

    pub fn logged_admin() -> CurrentAccountSubject {
        let account_name = odf::AccountName::new_unchecked("admin");
        let account_id = odf::AccountID::new_seeded_ed25519(account_name.as_bytes());
        let is_admin = true;

        CurrentAccountSubject::logged(account_id, account_name, is_admin)
    }

    pub fn anonymous() -> CurrentAccountSubject {
        CurrentAccountSubject::anonymous(AnonymousAccountReason::NoAuthenticationProvided)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
