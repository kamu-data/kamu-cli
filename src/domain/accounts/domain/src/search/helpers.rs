// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::SearchSecurityContext;

use crate::Principal;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `is_admin` matters only for [`Principal::User`]
pub fn make_search_security_context(
    principal: &Principal,
    is_admin: bool,
) -> SearchSecurityContext {
    match principal {
        Principal::User(account_handle) => {
            if is_admin {
                SearchSecurityContext::Unrestricted
            } else {
                SearchSecurityContext::Restricted {
                    current_principal_ids: vec![account_handle.id.to_string()],
                }
            }
        }
        Principal::Node => SearchSecurityContext::Unrestricted,
        Principal::Anonymous => SearchSecurityContext::Anonymous,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;

    #[test]
    fn test_search_security_context_per_principal() {
        let user = Principal::new_test_with(&"alice");
        let Principal::User(account_handle) = &user else {
            unreachable!()
        };
        let account_resource_id = account_handle.id.to_string();

        assert_matches!(
            make_search_security_context(&user, false),
            SearchSecurityContext::Restricted { current_principal_ids }
                if current_principal_ids == [account_resource_id]
        );
        assert_matches!(
            make_search_security_context(&user, true),
            SearchSecurityContext::Unrestricted
        );
        assert_matches!(
            make_search_security_context(&Principal::Node, false),
            SearchSecurityContext::Unrestricted
        );
        assert_matches!(
            make_search_security_context(&Principal::Anonymous, true),
            SearchSecurityContext::Anonymous
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
