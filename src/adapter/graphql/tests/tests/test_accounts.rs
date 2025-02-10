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
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME_STR, DUMMY_EMAIL_ADDRESS};

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_id() {
    let invalid_account_id = odf::AccountID::new_seeded_ed25519(b"I don't exist");

    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_anonymous.clone()),
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
            .data(harness.catalog_anonymous),
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
    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_anonymous.clone()),
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
            .data(harness.catalog_anonymous.clone()),
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
            .data(harness.catalog_anonymous),
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
    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_authorized),
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
            .data(harness.catalog_anonymous.clone()),
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
            .data(harness.catalog_anonymous),
        )
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Account access error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_success() {
    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_authorized),
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
    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_authorized),
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
    let harness = GraphQLAccountsHarness::new().await;

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
            .data(harness.catalog_anonymous),
        )
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Account access error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub async fn new() -> Self {
        let mut b = dill::CatalogBuilder::new();
        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog).await;

        Self {
            catalog_anonymous,
            catalog_authorized,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
