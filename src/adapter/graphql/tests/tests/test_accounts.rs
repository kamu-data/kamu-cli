// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use database_common::NoOpDatabasePlugin;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{
    Account,
    AuthenticationService,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
    DEFAULT_ACCOUNT_NAME_STR,
    DUMMY_EMAIL_ADDRESS,
};
use mockall::predicate::eq;

use crate::utils::authentication_catalogs;

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

#[test_log::test(tokio::test)]
async fn test_account_attributes() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));
    mock_authentication_service
        .expect_account_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(Account::dummy())));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);
    let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&harness.catalog).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byId (accountId: "{}") {{
                            id
                            accountName
                            displayName
                            accountType
                            email
                            avatarUrl
                            isAdmin
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID
            ))
            .data(catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "id": DEFAULT_ACCOUNT_ID.to_string(),
                    "accountName": DEFAULT_ACCOUNT_NAME_STR,
                    "displayName": DEFAULT_ACCOUNT_NAME_STR,
                    "accountType": "USER",
                    "email": DUMMY_EMAIL_ADDRESS.as_ref(),
                    "avatarUrl": None::<Option<&str>>,
                    "isAdmin": false,
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
                        byId (accountId: "{}") {{
                            id
                            accountName
                            displayName
                            accountType
                            avatarUrl
                            isAdmin
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID
            ))
            .data(catalog_anonymous.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "id": DEFAULT_ACCOUNT_ID.to_string(),
                    "accountName": DEFAULT_ACCOUNT_NAME_STR,
                    "displayName": DEFAULT_ACCOUNT_NAME_STR,
                    "accountType": "USER",
                    "avatarUrl": None::<Option<&str>>,
                    "isAdmin": false,
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
                        byId (accountId: "{}") {{
                            email
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID
            ))
            .data(catalog_anonymous.clone()),
        )
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Account access error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_success() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));
    mock_authentication_service
        .expect_account_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(Account::dummy())));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);
    let (_, catalog_authorized) = authentication_catalogs(&harness.catalog).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            updateEmail(newEmail: "{}") {{
                                message
                                ... on UpdateEmailSuccess {{
                                    newEmail
                                }}
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "wasya@example.com"
            ))
            .data(catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "updateEmail": {
                        "message": "Success",
                        "newEmail": "wasya@example.com"
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_bad_email() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));
    mock_authentication_service
        .expect_account_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(Account::dummy())));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);
    let (_, catalog_authorized) = authentication_catalogs(&harness.catalog).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            updateEmail(newEmail: "{}") {{
                                message
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "wasya#example.com"
            ))
            .data(catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "updateEmail": {
                        "message": "Invalid email",
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_unauthorized() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_find_account_name_by_id()
        .with(eq(DEFAULT_ACCOUNT_ID.clone()))
        .returning(|_| Ok(Some(DEFAULT_ACCOUNT_NAME.clone())));

    let harness = GraphQLAccountsHarness::new(mock_authentication_service);
    let (catalog_anonymous, _) = authentication_catalogs(&harness.catalog).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            updateEmail(newEmail: "{}") {{
                                message
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "wasya#example.com"
            ))
            .data(catalog_anonymous.clone()),
        )
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Account access error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub fn new(mock_authentication_service: MockAuthenticationService) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add_value(mock_authentication_service)
            .bind::<dyn AuthenticationService, MockAuthenticationService>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self { catalog }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
