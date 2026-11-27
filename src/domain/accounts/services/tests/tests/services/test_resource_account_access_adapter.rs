// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{AccountHandle, CurrentAccountSubject, Principal};
use kamu_accounts_services::ResourceAccountAccessAdapter;
use kamu_auth_rebac::{AccountPropertyName, RebacService};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_resources::{ResourceAccountAccessPort, ResourceOperationError};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const OWNER: &str = "owner";
const ADMIN: &str = "admin";
const OTHER: &str = "other";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_check_can_use_account() {
    let harness = ResourceAccountAccessHarness::new().await;
    let denied = format!("Current subject is not allowed to use resources of account '{OWNER}'");

    let cases = [
        ("node", ResourceAccountAccessHarness::node(), None),
        (
            "own account",
            ResourceAccountAccessHarness::user(OWNER),
            None,
        ),
        ("admin", ResourceAccountAccessHarness::user(ADMIN), None),
        (
            "other user",
            ResourceAccountAccessHarness::user(OTHER),
            Some(&denied),
        ),
        (
            "service account of the account",
            ResourceAccountAccessHarness::service_account_of(OWNER),
            Some(&denied),
        ),
    ];

    for (case, principal, expected_denial) in cases {
        let result = harness.check_can_use_account(&principal, OWNER).await;

        match expected_denial {
            None => assert_matches!(result, Ok(()), "{case}"),
            Some(expected_message) => {
                assert_matches!(
                    result,
                    Err(ResourceOperationError::Access(odf::AccessError::Unauthorized(e)))
                        if e.to_string() == *expected_message,
                    "{case}"
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceAccountAccessHarness {
    catalog: dill::Catalog,
}

impl ResourceAccountAccessHarness {
    async fn new() -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<ResourceAccountAccessAdapter>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default());

        let harness = Self { catalog: b.build() };
        harness.make_admin(ADMIN).await;
        harness
    }

    fn account_handle(account_name: &str) -> AccountHandle {
        CurrentAccountSubject::new_test_with(&account_name)
            .account_handle()
            .clone()
    }

    fn node() -> odf::metadata::auth::PrincipalHandle {
        Principal::Node.handle().unwrap()
    }

    fn user(account_name: &str) -> odf::metadata::auth::PrincipalHandle {
        Principal::User(Self::account_handle(account_name))
            .handle()
            .unwrap()
    }

    /// A non-user principal carrying the account's own DID
    fn service_account_of(account_name: &str) -> odf::metadata::auth::PrincipalHandle {
        odf::metadata::auth::PrincipalHandle {
            r#type: odf::metadata::auth::ServiceAccount::type_uri().clone(),
            account: Some(Self::account_handle(account_name).into()),
            ..Self::user(account_name)
        }
    }

    async fn make_admin(&self, account_name: &str) {
        let (name, value) = AccountPropertyName::is_admin(true);
        self.catalog
            .get_one::<dyn RebacService>()
            .unwrap()
            .set_account_property(&Self::account_handle(account_name).did, name, &value)
            .await
            .unwrap();
    }

    async fn check_can_use_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account_name: &str,
    ) -> Result<(), ResourceOperationError> {
        self.catalog
            .get_one::<dyn ResourceAccountAccessPort>()
            .unwrap()
            .check_can_use_account(principal, &Self::account_handle(account_name).into())
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
