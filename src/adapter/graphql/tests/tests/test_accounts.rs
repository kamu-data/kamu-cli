// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{
    Account,
    AuthenticationService,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
    DEFAULT_ACCOUNT_NAME_STR,
};
use mockall::predicate::eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_id() {
    let invalid_account_id = odf::AccountID::new_seeded_ed25519(b"I don't exist");

    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(invalid_account_id.clone()))
        .returning(|_| Ok(None));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byId (accountId: "{}") {{
                            accountName
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID
            ))
            .data(harness.catalog.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                }
            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byId (accountId: "{invalid_account_id}") {{
                            accountName
                        }}
                    }}
                }}
                "#,
            ))
            .data(harness.catalog.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": null
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_name() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_account_by_name()
        .with(eq(DEFAULT_ACCOUNT_NAME.clone()))
        .returning(|_| Ok(Some(Account::dummy())));
    mock_authentication_service
        .expect_account_by_name()
        .with(eq(odf::AccountName::new_unchecked("unknown")))
        .returning(|_| Ok(None));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byName (name: "{DEFAULT_ACCOUNT_NAME_STR}") {{
                            accountName
                        }}
                    }}
                }}
                "#,
            ))
            .data(harness.catalog.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byName": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                }
            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byName (name: "{}") {{
                            accountName
                        }}
                    }}
                }}
                "#,
                "unknown",
            ))
            .data(harness.catalog.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byName": null
            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byName (name: "{}") {{
                            accountName
                        }}
                    }}
                }}
                "#,
                DEFAULT_ACCOUNT_NAME_STR.to_ascii_uppercase(),
            ))
            .data(harness.catalog),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byName": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub fn new(mock_authentication_service: MockAuthenticationService) -> Self {
        let catalog = dill::CatalogBuilder::new()
            .add_value(mock_authentication_service)
            .bind::<dyn AuthenticationService, MockAuthenticationService>()
            .build();

        Self { catalog }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
