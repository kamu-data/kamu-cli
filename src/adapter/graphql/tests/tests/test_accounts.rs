// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use dill::Component;
use indoc::indoc;
use kamu_accounts::*;
use kamu_adapter_graphql::traits::ResponseExt;

use crate::utils::{PredefinedAccountOpts, authentication_catalogs};

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

    pretty_assertions::assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_own_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let default_test_account = CurrentAccountSubject::new_test();

    let res = schema
        .execute(
            delete_account_request(default_test_account.account_name())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "delete": {
                        "__typename": "DeleteAccountSuccess",
                        "message": "Account deleted",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    let res = schema
        .execute(
            account_by_name_request(default_test_account.account_name())
                .data(harness.catalog_authorized),
        )
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_delete_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        can_provision_accounts: true,
        is_admin: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let another_account_username = "another-user";

    let res = schema
        .execute(
            create_account_request(another_account_username)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            delete_account_request(another_account_username)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "delete": {
                        "__typename": "DeleteAccountSuccess",
                        "message": "Account deleted",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    let res = schema
        .execute(account_by_name_request(another_account_username).data(harness.catalog_authorized))
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_try_to_delete_account() {
    let harness = GraphQLAccountsHarness::new(Default::default()).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let default_test_account = CurrentAccountSubject::new_test();

    let res = schema
        .execute(
            delete_account_request(default_test_account.account_name())
                .data(harness.catalog_anonymous),
        )
        .await;
    pretty_assertions::assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_delete_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let another_account_username = "another-user";

    let res = schema
        .execute(
            create_account_request(another_account_username)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(delete_account_request(another_account_username).data(harness.catalog_authorized))
        .await;

    pretty_assertions::assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rename_own_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let default_test_account = CurrentAccountSubject::new_test();
    let default_password = AccountConfig::generate_password(default_test_account.account_name());

    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = schema
        .execute(
            rename_account_request(
                default_test_account.account_name(),
                RENAMED_ANOTHER_ACCOUNT_NAME,
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "rename": {
                        "__typename": "RenameAccountSuccess",
                        "message": "Account renamed",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    let res = schema
        .execute(
            account_by_name_request(default_test_account.account_name())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );

    let res = schema
        .execute(
            account_by_name_request(RENAMED_ANOTHER_ACCOUNT_NAME).data(harness.catalog_authorized),
        )
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": {
                    "accountName": RENAMED_ANOTHER_ACCOUNT_NAME
                }
            }
        }),
        "{res:?}"
    );

    // Try login with the new name
    let res = schema
        .execute(
            login_via_password_request(
                // login is new, the renamed account name
                RENAMED_ANOTHER_ACCOUNT_NAME,
                // password is still the same
                &default_password,
            )
            .data(harness.catalog_anonymous.clone()),
        )
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "auth": {
                "login": {
                    "account": {
                        "accountName": RENAMED_ANOTHER_ACCOUNT_NAME
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    // An attempt to login under the old name should fail
    let res = schema
        .execute(
            login_via_password_request(default_test_account.account_name(), &default_password)
                .data(harness.catalog_anonymous.clone()),
        )
        .await;
    assert!(res.is_err(), "{res:?}");
    assert!(res.errors.len() == 1);
    assert_eq!(
        res.errors[0].message,
        "Rejected credentials: invalid login or password".to_string()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rename_own_account_taken_name() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let default_test_account = CurrentAccountSubject::new_test();
    const TAKEN_ACCOUNT_NAME: &str = "taken-account-name";

    let res = schema
        .execute(
            create_account_request(TAKEN_ACCOUNT_NAME).data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            rename_account_request(default_test_account.account_name(), TAKEN_ACCOUNT_NAME)
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "rename": {
                        "__typename": "RenameAccountNameNotUnique",
                        "message": "Non-unique account name",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    // Idempotent behavior
    let res = schema
        .execute(
            rename_account_request(
                default_test_account.account_name(),
                default_test_account.account_name(),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "rename": {
                        "__typename": "RenameAccountSuccess",
                        "message": "Account renamed",
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
async fn test_admin_renames_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        can_provision_accounts: true,
        is_admin: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    const ANOTHER_ACCOUNT_NAME: &str = "another-user";
    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = schema
        .execute(
            create_account_request(ANOTHER_ACCOUNT_NAME).data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            rename_account_request(ANOTHER_ACCOUNT_NAME, RENAMED_ANOTHER_ACCOUNT_NAME)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    pretty_assertions::assert_eq!(
        &value!({
            "accounts": {
                "byName": {
                    "rename": {
                        "__typename": "RenameAccountSuccess",
                        "message": "Account renamed",
                    }
                }
            }
        }),
        &res.data,
        "{res:?}"
    );

    let res = schema
        .execute(
            account_by_name_request(ANOTHER_ACCOUNT_NAME).data(harness.catalog_authorized.clone()),
        )
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );

    let res = schema
        .execute(
            account_by_name_request(RENAMED_ANOTHER_ACCOUNT_NAME).data(harness.catalog_authorized),
        )
        .await;
    pretty_assertions::assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": {
                    "accountName": RENAMED_ANOTHER_ACCOUNT_NAME
                }
            }
        }),
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_try_to_rename_account() {
    let harness = GraphQLAccountsHarness::new(Default::default()).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let default_test_account = CurrentAccountSubject::new_test();

    let res = schema
        .execute(
            rename_account_request(default_test_account.account_name(), "renamed-user")
                .data(harness.catalog_anonymous),
        )
        .await;
    pretty_assertions::assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_rename_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    const ANOTHER_ACCOUNT_NAME: &str = "another-user";
    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = schema
        .execute(
            create_account_request(ANOTHER_ACCOUNT_NAME).data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = schema
        .execute(
            rename_account_request(ANOTHER_ACCOUNT_NAME, RENAMED_ANOTHER_ACCOUNT_NAME)
                .data(harness.catalog_authorized),
        )
        .await;

    pretty_assertions::assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub async fn new(predefined_account_opts: PredefinedAccountOpts) -> Self {
        let mut b = dill::CatalogBuilder::new();
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();
        let final_catalog = dill::CatalogBuilder::new_chained(&catalog)
            .add_value(kamu_core::TenancyConfig::MultiTenant)
            .add::<kamu_accounts_inmem::InMemoryAccessTokenRepository>()
            .add::<kamu_accounts_inmem::InMemoryDidSecretKeyRepository>()
            .add::<kamu_accounts_inmem::InMemoryOAuthDeviceCodeRepository>()
            .add::<kamu_accounts_services::AccessTokenServiceImpl>()
            .add::<kamu_accounts_services::AuthenticationServiceImpl>()
            .add::<kamu_accounts_services::CreateAccountUseCaseImpl>()
            .add::<kamu_accounts_services::ModifyAccountPasswordUseCaseImpl>()
            .add::<kamu_accounts_services::DeleteAccountUseCaseImpl>()
            .add::<kamu_accounts_services::UpdateAccountEmailUseCaseImpl>()
            .add::<kamu_accounts_services::RenameAccountUseCaseImpl>()
            .add::<kamu_accounts_services::OAuthDeviceCodeGeneratorDefault>()
            .add::<kamu_accounts_services::OAuthDeviceCodeServiceImpl>()
            .add::<kamu_accounts_services::utils::AccountAuthorizationHelperImpl>()
            .add::<time_source::SystemTimeSourceDefault>()
            .add_value(JwtAuthenticationConfig::default())
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn messaging_outbox::Outbox, messaging_outbox::OutboxImmediateImpl>()
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
    async_graphql::Request::new(indoc!(
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
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "newAccountName": new_account_name,
    })))
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

fn account_by_name_request(account_name: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        query ($accountName: AccountName!) {
          accounts {
            byName(name: $accountName) {
              accountName
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountName": account_name,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn delete_account_request(account_name: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation ($accountName: AccountName!) {
          accounts {
            byName(accountName: $accountName) {
              delete {
                __typename
                message
              }
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountName": account_name,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn rename_account_request(account_name: &str, new_account_name: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation ($accountName: AccountName!, $newAccountName: AccountName!) {
          accounts {
            byName(accountName: $accountName) {
              rename(newName: $newAccountName) {
                __typename
                message
              }
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountName": account_name,
        "newAccountName": new_account_name,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn login_via_password_request(account_name: &str, password: &str) -> async_graphql::Request {
    let credentials = format!(r#"{{"login": "{account_name}", "password": "{password}"}}"#,);

    async_graphql::Request::new(indoc!(
        r#"
        mutation($credentials: String!) {
          auth {
            login(loginMethod: PASSWORD, loginCredentialsJson: $credentials) {
              account {
                accountName
              }
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "credentials": credentials,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
