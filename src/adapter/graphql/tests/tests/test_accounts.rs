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
use pretty_assertions::assert_eq;
use serde_json::json;

use crate::utils::{GraphQLQueryRequest, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_id() {
    let invalid_account_id = odf::AccountID::new_seeded_ed25519(b"I don't exist");

    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let request_code = indoc!(
        r#"
        query ($accountId: AccountID!) {
          accounts {
            byId(accountId: $accountId) {
              accountName
            }
          }
        }
        "#,
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(value!({
                "accountId": *DEFAULT_ACCOUNT_ID
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
        value!({
            "accounts": {
                "byId": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                }
            }
        })
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(value!({
                "accountId": invalid_account_id
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
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

    let request_code = indoc!(
        r#"
        query ($accountName: AccountName!) {
          accounts {
            byName(name: $accountName) {
              accountName
            }
          }
        }
        "#,
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(value!({
                "accountName": DEFAULT_ACCOUNT_NAME_STR
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
        value!({
            "accounts": {
                "byName": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                }
            }
        })
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(value!({
                "accountName": DEFAULT_ACCOUNT_NAME_STR
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
        value!({
            "accounts": {
                "byName": {
                    "accountName": "unknown"
                }
            }
        })
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(value!({
                "accountName": DEFAULT_ACCOUNT_NAME_STR.to_ascii_uppercase()
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
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

    assert_eq!(
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query ($accountId: AccountID!) {
                  accounts {
                    byId(accountId: $accountId) {
                      id
                      accountName
                      displayName
                      accountType
                      email
                      avatarUrl
                      isAdmin
                    }
                  }
                }
                "#,
            ),
            async_graphql::Variables::from_value(value!({
                "accountId": *DEFAULT_ACCOUNT_ID
            })),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data,
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

    assert_eq!(
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query ($accountId: AccountID!) {
                  accounts {
                    byId(accountId: $accountId) {
                      id
                      accountName
                      displayName
                      accountType
                      avatarUrl
                      isAdmin
                    }
                  }
                }
                "#,
            ),
            async_graphql::Variables::from_value(value!({
                "accountId": *DEFAULT_ACCOUNT_ID
            })),
        )
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .data,
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

    assert_eq!(
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query ($accountId: AccountID!) {
                  accounts {
                    byId(accountId: $accountId) {
                      email
                    }
                  }
                }
                "#,
            ),
            async_graphql::Variables::from_value(value!({
                "accountId": *DEFAULT_ACCOUNT_ID
            })),
        )
        .expect_error()
        .execute(&schema, &harness.catalog_anonymous)
        .await
        .error_messages(),
        ["Account access error"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_email_success() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(format!(
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
        )))
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

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(format!(
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
        )))
        .await;

    assert_eq!(
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

    // Create an account
    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_anonymous_query(
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
            }))),
        )
        .await;

    assert_eq!(["Account access error"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;

    assert_eq!(
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
    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;

    assert_eq!(
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

    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;

    assert_eq!(
        ["Account is not authorized to provision accounts"],
        *res.error_messages(),
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_wallet_accounts() {
    fn sort_accounts(response_root_json: &mut serde_json::Value) {
        let accounts = response_root_json["accounts"]["createWalletAccounts"]["accounts"]
            .as_array_mut()
            .unwrap();
        accounts.sort_by(|a, b| {
            let a_name = a["accountName"].as_str().unwrap();
            let b_name = b["accountName"].as_str().unwrap();
            a_name.cmp(b_name)
        });
    }

    // -----

    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let wallet_accounts = [
        "did:pkh:eip155:1:0xbf9a00755BB7d2E904b5F569095220c54E742E07",
        "did:pkh:eip155:1:0x3F41642f9813eb3be2DBc098dad815c25b9156E8",
        "did:pkh:eip155:1:0xeCd666A695086c10D8d4AB146D2827842bd15Ef9",
    ];
    let expected_result = json!({
        "accounts": {
            "createWalletAccounts": {
                "message": "Wallet accounts created",
                "accounts": [
                    {
                        "accountName": "0x3F41642f9813eb3be2DBc098dad815c25b9156E8",
                        "displayName": "0x3F41642f9813eb3be2DBc098dad815c25b9156E8",
                        "accountProvider": "WEB3_WALLET",
                    },
                    {
                        "accountName": "0xbf9a00755BB7d2E904b5F569095220c54E742E07",
                        "displayName": "0xbf9a00755BB7d2E904b5F569095220c54E742E07",
                        "accountProvider": "WEB3_WALLET",
                    },
                    {
                        "accountName": "0xeCd666A695086c10D8d4AB146D2827842bd15Ef9",
                        "displayName": "0xeCd666A695086c10D8d4AB146D2827842bd15Ef9",
                        "accountProvider": "WEB3_WALLET",
                    },
                ]
            }
        }
    });

    {
        let res = harness
            .execute_authorized_query(create_wallet_accounts_request(&wallet_accounts))
            .await;
        assert!(res.is_ok(), "{res:?}");

        let mut root_json = res.data.into_json().unwrap();
        sort_accounts(&mut root_json);
        assert_eq!(&expected_result, &root_json);
    }
    // Idempotence.
    {
        let res = harness
            .execute_authorized_query(create_wallet_accounts_request(&wallet_accounts))
            .await;

        assert!(res.is_ok(), "{res:?}");

        let mut root_json = res.data.into_json().unwrap();
        sort_accounts(&mut root_json);
        assert_eq!(&expected_result, &root_json);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_wallet_accounts_without_permissions() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: false,
    })
    .await;

    let wallet_accounts = [
        "did:pkh:eip155:1:0xbf9a00755BB7d2E904b5F569095220c54E742E07",
        "did:pkh:eip155:1:0x3F41642f9813eb3be2DBc098dad815c25b9156E8",
        "did:pkh:eip155:1:0xeCd666A695086c10D8d4AB146D2827842bd15Ef9",
    ];

    let res = harness
        .execute_authorized_query(create_wallet_accounts_request(&wallet_accounts))
        .await;

    assert_eq!(
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

    // Create an account
    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify the password for the account
    let res = harness
        .execute_authorized_query(modify_password_request("foo", "p4s5w0rd"))
        .await;
    assert_eq!(
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

    // Create an account
    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;
    assert!(res.is_ok(), "{res:?}");

    for (password, expected_error_message) in [
        ("", "Minimum password length is 8"),
        ("foo±password", "Password contains non-ASCII characters"),
    ] {
        let res = harness
            .execute_authorized_query(
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
                }))),
            )
            .await;

        assert_eq!(
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

    // Create an account
    let res = harness
        .execute_authorized_query(create_account_request("foo"))
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Modify the password for the account
    let res = harness
        .execute_authorized_query(
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
            }))),
        )
        .await;

    assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_own_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let default_test_account = CurrentAccountSubject::new_test();

    let res = harness
        .execute_authorized_query(delete_account_request(default_test_account.account_name()))
        .await;
    assert_eq!(
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

    let res = harness
        .execute_authorized_query(account_by_name_request(default_test_account.account_name()))
        .await;
    assert_eq!(
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

    let another_account_username = "another-user";

    let res = harness
        .execute_authorized_query(create_account_request(another_account_username))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_authorized_query(delete_account_request(another_account_username))
        .await;
    assert_eq!(
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

    let res = harness
        .execute_authorized_query(account_by_name_request(another_account_username))
        .await;
    assert_eq!(
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

    let default_test_account = CurrentAccountSubject::new_test();

    let res = harness
        .execute_anonymous_query(delete_account_request(default_test_account.account_name()))
        .await;
    assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_delete_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let another_account_username = "another-user";

    let res = harness
        .execute_authorized_query(create_account_request(another_account_username))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_authorized_query(delete_account_request(another_account_username))
        .await;

    assert_eq!(["Unauthenticated"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rename_own_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    let default_test_account = CurrentAccountSubject::new_test();
    let default_password = AccountConfig::generate_password(default_test_account.account_name());

    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = harness
        .execute_authorized_query(rename_account_request(
            default_test_account.account_name(),
            RENAMED_ANOTHER_ACCOUNT_NAME,
        ))
        .await;

    assert_eq!(
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

    let res = harness
        .execute_authorized_query(account_by_name_request(default_test_account.account_name()))
        .await;
    assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );

    let res = harness
        .execute_authorized_query(account_by_name_request(RENAMED_ANOTHER_ACCOUNT_NAME))
        .await;
    assert_eq!(
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
    let res = harness
        .execute_anonymous_query(login_via_password_request(
            // login is new, the renamed account name
            RENAMED_ANOTHER_ACCOUNT_NAME,
            // password is still the same
            &default_password,
        ))
        .await;

    assert_eq!(
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
    let res = harness
        .execute_anonymous_query(login_via_password_request(
            default_test_account.account_name(),
            &default_password,
        ))
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

    let default_test_account = CurrentAccountSubject::new_test();
    const TAKEN_ACCOUNT_NAME: &str = "taken-account-name";

    let res = harness
        .execute_authorized_query(create_account_request(TAKEN_ACCOUNT_NAME))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_authorized_query(rename_account_request(
            default_test_account.account_name(),
            TAKEN_ACCOUNT_NAME,
        ))
        .await;

    assert_eq!(
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
    let res = harness
        .execute_authorized_query(rename_account_request(
            default_test_account.account_name(),
            default_test_account.account_name(),
        ))
        .await;

    assert_eq!(
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

    const ANOTHER_ACCOUNT_NAME: &str = "another-user";
    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = harness
        .execute_authorized_query(create_account_request(ANOTHER_ACCOUNT_NAME))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_authorized_query(rename_account_request(
            ANOTHER_ACCOUNT_NAME,
            RENAMED_ANOTHER_ACCOUNT_NAME,
        ))
        .await;
    assert_eq!(
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

    let res = harness
        .execute_authorized_query(account_by_name_request(ANOTHER_ACCOUNT_NAME))
        .await;
    assert_eq!(
        &res.data,
        &value!({
            "accounts": {
                "byName": null
            }
        }),
        "{res:?}"
    );

    let res = harness
        .execute_authorized_query(account_by_name_request(RENAMED_ANOTHER_ACCOUNT_NAME))
        .await;
    assert_eq!(
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

    let default_test_account = CurrentAccountSubject::new_test();

    let res = harness
        .execute_anonymous_query(rename_account_request(
            default_test_account.account_name(),
            "renamed-user",
        ))
        .await;
    assert_eq!(["Unauthorized"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_rename_other_account() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts {
        is_admin: false,
        can_provision_accounts: true,
    })
    .await;

    const ANOTHER_ACCOUNT_NAME: &str = "another-user";
    const RENAMED_ANOTHER_ACCOUNT_NAME: &str = "renamed-another-user";

    let res = harness
        .execute_authorized_query(create_account_request(ANOTHER_ACCOUNT_NAME))
        .await;
    assert!(res.is_ok(), "{res:?}");

    let res = harness
        .execute_authorized_query(rename_account_request(
            ANOTHER_ACCOUNT_NAME,
            RENAMED_ANOTHER_ACCOUNT_NAME,
        ))
        .await;

    assert_eq!(["Unauthorized"], *res.error_messages(), "{res:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_get_access_token() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

    let mutation_request = create_access_token_request(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = harness.execute_authorized_query(mutation_request).await;

    assert!(res.is_ok());

    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();

    let created_token_id =
        json["accounts"]["byId"]["accessTokens"]["createAccessToken"]["token"]["id"].clone();

    let query_request = get_access_tokens_request(&DEFAULT_ACCOUNT_ID.to_string());
    let res = harness.execute_authorized_query(query_request).await;

    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "accessTokens": {
                        "listAccessTokens": {
                            "nodes": [{
                                "id": created_token_id,
                                "name": "foo",
                                "revokedAt": null
                            }]
                        }
                    }
                }
            }
        })
    );

    let mutation_request = create_access_token_request(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = harness.execute_authorized_query(mutation_request).await;

    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "accessTokens": {
                        "createAccessToken": {
                            "__typename": "CreateAccessTokenResultDuplicate",
                            "message": "Access token with foo name already exists"
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_revoke_access_token() {
    let harness = GraphQLAccountsHarness::new(PredefinedAccountOpts::default()).await;

    let mutation_request = create_access_token_request(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = harness.execute_authorized_query(mutation_request).await;

    assert!(res.is_ok());

    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();

    let created_token_id =
        json["accounts"]["byId"]["accessTokens"]["createAccessToken"]["token"]["id"].clone();

    let mutation_request = revoke_access_token_request(
        &DEFAULT_ACCOUNT_ID.to_string(),
        &created_token_id.to_string(),
    );

    let res = harness.execute_anonymous_query(mutation_request).await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        "Access token access error".to_string()
    );

    let mutation_request = revoke_access_token_request(
        &DEFAULT_ACCOUNT_ID.to_string(),
        &created_token_id.to_string(),
    );

    let res = harness.execute_authorized_query(mutation_request).await;

    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "accessTokens": {
                        "revokeAccessToken": {
                            "__typename": "RevokeResultSuccess",
                            "message": "Access token revoked successfully"
                        }
                    }
                }
            }
        })
    );

    let mutation_request = revoke_access_token_request(
        &DEFAULT_ACCOUNT_ID.to_string(),
        &created_token_id.to_string(),
    );

    let res = harness.execute_authorized_query(mutation_request).await;

    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "accounts": {
                "byId": {
                    "accessTokens": {
                        "revokeAccessToken": {
                            "__typename": "RevokeResultAlreadyRevoked",
                        }
                    }
                }
            }
        })
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
            .add::<kamu_accounts_services::UpdateAccountUseCaseImpl>()
            .add::<kamu_accounts_services::OAuthDeviceCodeGeneratorDefault>()
            .add::<kamu_accounts_services::OAuthDeviceCodeServiceImpl>()
            .add::<kamu_accounts_services::utils::AccountAuthorizationHelperImpl>()
            .add::<kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl>()
            .add::<time_source::SystemTimeSourceDefault>()
            .add_value(JwtAuthenticationConfig::default())
            .add_value(AuthConfig::sample())
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

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(&self.catalog_authorized))
                    .data(self.catalog_authorized.clone()),
            )
            .await
    }

    pub async fn execute_anonymous_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(&self.catalog_authorized))
                    .data(self.catalog_anonymous.clone()),
            )
            .await
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

fn create_wallet_accounts_request(wallet_accounts: &[&str]) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation ($newWalletAccounts: [DidPkh!]!) {
          accounts {
            createWalletAccounts(walletAddresses: $newWalletAccounts) {
              ... on CreateWalletAccountsSuccess {
                message
                accounts {
                  accountName
                  displayName
                  accountProvider
                }
              }
            }
          }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "newWalletAccounts": wallet_accounts,
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

fn create_access_token_request(account_id: &str, token_name: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation($accountId: String!, $tokenName: String!) {
            accounts {
                byId(accountId: $accountId) {
                    accessTokens {
                        createAccessToken (tokenName: $tokenName) {
                            __typename
                            message
                            ... on CreateAccessTokenResultSuccess {
                                token {
                                    id,
                                    name,
                                    composed
                                }
                            }
                        }
                    }
                }
            }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountId": account_id,
        "tokenName": token_name,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn revoke_access_token_request(account_id: &str, token_id: &str) -> async_graphql::Request {
    let query = indoc!(
        r#"
        mutation {
            accounts {
                byId(accountId: "<account_id>") {
                    accessTokens {
                        revokeAccessToken (tokenId: <token_id>) {
                            __typename
                            ... on RevokeResultSuccess {
                                message
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<account_id>", account_id)
    .replace("<token_id>", token_id);

    async_graphql::Request::new(query)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_access_tokens_request(account_id: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        query($accountId: String!) {
            accounts {
                byId(accountId: $accountId) {
                    accessTokens {
                        listAccessTokens (perPage: 10, page: 0) {
                            nodes {
                                id,
                                name,
                                revokedAt
                            }
                        }
                    }
                }
            }
        }
        "#,
    ))
    .variables(async_graphql::Variables::from_value(value!({
        "accountId": account_id,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
