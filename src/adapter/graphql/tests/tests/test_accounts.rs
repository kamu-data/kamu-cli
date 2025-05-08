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
use dill::Component;
use indoc::indoc;
use kamu_accounts::{
    JwtAuthenticationConfig,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME_STR,
    DUMMY_EMAIL_ADDRESS,
};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryOAuthDeviceCodeRepository};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    CreateAccountUseCaseImpl,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
};
use kamu_did_secret_keys_inmem::InMemoryDidSecretKeyRepository;
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use time_source::SystemTimeSourceDefault;

use crate::utils::{authentication_catalogs, PredefinedAccountOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_id() {
    let invalid_account_id = odf::AccountID::new_seeded_ed25519(b"I don't exist");

    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

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

#[test_log::test(tokio::test)]
async fn test_create_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let gql_query = format!(
        r#"
        mutation {{
            accounts {{
                byId (accountId: "{}") {{
                    createAccount(accountName: "{}") {{
                        message
                        ... on CreateAccountSuccess {{
                            account {{
                                accountName
                                displayName
                                accountType
                            }}
                        }}
                    }}
                }}
            }}
        }}
        "#,
        *DEFAULT_ACCOUNT_ID, "foo"
    );
    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(gql_query.clone()).data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "createAccount": {
                        "message": "Account created",
                        "account": {
                            "accountName": "foo",
                            "displayName": "foo",
                            "accountType": "USER"
                        }
                    }
                }
            }
        })
    );

    // Creating account with the same name should return error
    let res = schema
        .execute(async_graphql::Request::new(gql_query).data(harness.catalog_authorized))
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "createAccount": {
                        "message": "Non-unique account field 'name'",
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account_without_permissions() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            createAccount(accountName: "{}") {{
                                ... on CreateAccountSuccess {{
                                    message
                                    account {{
                                        accountName
                                        displayName
                                        accountType
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo"
            ))
            .data(harness.catalog_authorized),
        )
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        "Account does not have permission to provision accounts".to_string()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        can_provision_accounts: true,
        is_admin: true,
    })
    .await;

    let new_password = "foo_password";

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create account
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            createAccount(accountName: "{}") {{
                                ... on CreateAccountSuccess {{
                                    message
                                    account {{
                                        accountName
                                        displayName
                                        accountType
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo"
            ))
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify password for account
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            modifyPassword(accountName: "{}", password: "{}") {{
                                message
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo", new_password
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
                    "modifyPassword": {
                        "message": "Password modified",
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password_errors() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        can_provision_accounts: true,
        is_admin: true,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create account
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            createAccount(accountName: "{}") {{
                                ... on CreateAccountSuccess {{
                                    message
                                    account {{
                                        accountName
                                        displayName
                                        accountType
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo"
            ))
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    for test_case in [
        ("", "Minimum password length is 8"),
        ("foo±password", "Password contains non-ASCII characters"),
    ] {
        let mutation_code = GraphQLAccountsHarness::modify_password(
            &DEFAULT_ACCOUNT_ID.to_string(),
            "foo",
            test_case.0,
        );

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code.clone())
                    .data(harness.catalog_authorized.clone()),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");
        assert_eq!(
            response.data,
            value!({
                    "accounts": {
                        "byId": {
                            "modifyPassword": {
                                "__typename": "ModifyPasswordInvalidPassword",
                                "message": test_case.1,
                            }
                        }
                    }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password_non_admin() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        can_provision_accounts: true,
        is_admin: false,
    })
    .await;

    let new_password = "foo_password";

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create account
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            createAccount(accountName: "{}") {{
                                ... on CreateAccountSuccess {{
                                    message
                                    account {{
                                        accountName
                                        displayName
                                        accountType
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo"
            ))
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify password for account
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                mutation {{
                    accounts {{
                        byId (accountId: "{}") {{
                            modifyPassword(accountName: "{}", password: "{}") {{
                                message
                            }}
                        }}
                    }}
                }}
                "#,
                *DEFAULT_ACCOUNT_ID, "foo", new_password
            ))
            .data(harness.catalog_authorized),
        )
        .await;

    assert!(res.is_err(), "{res:?}");
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        "Access restricted to administrators only".to_string()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub async fn new(predefined_account_opts: PredefinedAccountOpts) -> Self {
        let mut b = dill::CatalogBuilder::new();
        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();
        let final_catalog = dill::CatalogBuilder::new_chained(&catalog)
            .add::<AuthenticationServiceImpl>()
            .add::<AccessTokenServiceImpl>()
            .add::<CreateAccountUseCaseImpl>()
            .add::<InMemoryAccessTokenRepository>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add::<OAuthDeviceCodeServiceImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<OAuthDeviceCodeGeneratorDefault>()
            .add::<InMemoryOAuthDeviceCodeRepository>()
            .add_value(JwtAuthenticationConfig::default())
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .build();

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&final_catalog, predefined_account_opts).await;

        Self {
            catalog_anonymous,
            catalog_authorized,
        }
    }

    fn modify_password(account_id: &str, account_name: &str, password: &str) -> String {
        indoc!(
            r#"
            mutation {
                accounts {
                    byId (accountId: "<account_id>") {
                        modifyPassword(accountName: "<account_name>", password: "<password>") {
                            __typename
                            message
                        }
                    }
                }
            }
            "#
        )
        .replace("<account_id>", account_id)
        .replace("<account_name>", account_name)
        .replace("<password>", password)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
