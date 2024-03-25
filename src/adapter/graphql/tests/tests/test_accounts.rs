// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use kamu::testing::MockAuthenticationService;
use kamu_core::auth::{AccountInfo, DEFAULT_ACCOUNT_NAME};
use mockall::predicate::eq;
use opendatafabric::AccountName;

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_by_name() {
    let harness = GraphQLAccountsHarness::new();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(format!(
                r#"
                query {{
                    accounts {{
                        byName (name: "{DEFAULT_ACCOUNT_NAME}") {{
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
                    "accountName": DEFAULT_ACCOUNT_NAME
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
                DEFAULT_ACCOUNT_NAME.to_ascii_uppercase(),
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
                    "accountName": DEFAULT_ACCOUNT_NAME
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLAccountsHarness {
    catalog: dill::Catalog,
}

impl GraphQLAccountsHarness {
    pub fn new() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_find_account_info_by_name()
            .with(eq(AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME)))
            .returning(|_| Ok(Some(AccountInfo::dummy())));
        mock_authentication_service
            .expect_find_account_info_by_name()
            .with(eq(AccountName::new_unchecked("unknown")))
            .returning(|_| Ok(None));
        let catalog = dill::CatalogBuilder::new()
            .add_value(mock_authentication_service)
            .bind::<dyn kamu_core::auth::AuthenticationService, MockAuthenticationService>()
            .build();

        Self { catalog }
    }
}
