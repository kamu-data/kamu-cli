// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::SearchSecurityContext;

use crate::CurrentAccountSubject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn make_search_security_context(
    current_account_subject: &CurrentAccountSubject,
    is_admin: bool,
) -> SearchSecurityContext {
    match current_account_subject {
        CurrentAccountSubject::Logged(logged_account) => {
            if is_admin {
                SearchSecurityContext::Unrestricted
            } else {
                SearchSecurityContext::Restricted {
                    current_principal_ids: vec![logged_account.account_id.to_string()],
                }
            }
        }
        CurrentAccountSubject::Anonymous(_) => SearchSecurityContext::Anonymous,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
