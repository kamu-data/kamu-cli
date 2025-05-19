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
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    CreateAccountUseCaseImpl,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
};
use kamu_adapter_graphql::traits::ResponseExt;
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

    pretty_assertions::assert_eq!(
        ["Failed to parse \"Email\": wasya#example.com is not a valid email"],
        *res.error_messages(),
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_unauthorized() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    // Create an account
    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($accountName: AccountName!, $newEmail: Email!) {
                  accounts {
                    byName(accountName: $accountName) {
                      updateEmail(newEmail: $newEmail) {
                        message
                      }
                    }
                  }
                }
                "#,
            ))
            .variables(async_graphql::Variables::from_value(value!({
                "accountName": "foo",
                "newEmail": "foo@example.com",
            })))
            .data(harness.catalog_anonymous),
        )
        .await;

    pretty_assertions::assert_eq!(["Account access error"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized.clone()))
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "createAccount": {
                    "message": "Account created",
                    "account": {
                        "accountName": "foo",
                        "displayName": "foo",
                        "accountType": "USER"
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    // Creating an account with the same name should return an error
    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized))
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "createAccount": {
                    "message": "Non-unique account field 'name'",
                }
            }
        }),
        &res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account_without_permissions() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized))
        .await;

    pretty_assertions::assert_eq!(
        ["Account is not authorized to provision accounts"],
        *res.error_messages(),
        "{res:?}"
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

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create an account
    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify the password for the account
    let res = schema
        .execute(modify_password_request("foo", "p4s5w0rd").data(harness.catalog_authorized))
        .await;
    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "modifyPassword": {
                        "message": "Password modified",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
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

    // Create an account
    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{res:?}");

    for (password, expected_error_message) in [
        ("", "Minimum password length is 8"),
        ("foo±password", "Password contains non-ASCII characters"),
    ] {
        let res = schema
            .execute(
                async_graphql::Request::new(indoc!(
                    r#"
                    mutation ($accountName: AccountName!, $newPassword: AccountPassword!) {
                      accounts {
                        byName(accountName: $accountName) {
                          modifyPassword(password: $newPassword) {
                            message
                          }
                        }
                      }
                    }
                    "#,
                ))
                .variables(async_graphql::Variables::from_value(value!({
                    "accountName": "foo",
                    "newPassword": password,
                })))
                .data(harness.catalog_authorized.clone()),
            )
            .await;

        pretty_assertions::assert_eq!(
            [format!(
                "Failed to parse \"AccountPassword\": {expected_error_message}"
            )],
            *res.error_messages(),
            "{res:?}"
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

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create an account
    let res = schema
        .execute(create_account_request("foo").data(harness.catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify the password for the account
    let res = schema
        .execute(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($accountName: AccountName!, $newPassword: AccountPassword!) {
                  accounts {
                    byName(accountName: $accountName) {
                      modifyPassword(password: $newPassword) {
                        message
                      }
                    }
                  }
                }
                "#,
            ))
            .variables(async_graphql::Variables::from_value(value!({
                "accountName": "foo",
                "newPassword": "foo_password",
            })))
            .data(harness.catalog_authorized),
        )
        .await;

    pretty_assertions::assert_eq!(
        ["Access restricted to administrators only"],
        *res.error_messages(),
        "{res:?}"
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_account_request(new_account_name: &str) -> async_graphql::Request {
    async_graphql::from_value(async_graphql::value!({
        "query": indoc::indoc!(
            r#"
            mutation ($newAccountName: AccountName!) {
              accounts {
                createAccount(accountName: $newAccountName) {
                  ... on CreateAccountSuccess {
                    message
                    account {
                      accountName
                      displayName
                      accountType
                    }
                  }
                  ... on AccountFieldNonUnique {
                    message
                  }
                }
              }
            }
            "#
        ),
        "variables": {
            "newAccountName": new_account_name,
        }
    }))
    .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn modify_password_request(account_name: &str, new_password: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation ($accountName: AccountName!, $newPassword: AccountPassword!) {
          accounts {
            byName(accountName: $accountName) {
              modifyPassword(password: $newPassword) {
                message
              }
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountName": account_name,
        "newPassword": new_password,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
