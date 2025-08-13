// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_cli_e2e_common::{E2E_USER_ACCOUNT_NAME_STR, KamuApiServerClient, KamuApiServerClientExt};
use odf::AccountName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_dataset_by_name(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    let foo1_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("foo1"),
    };
    let foo2_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("foo2"),
    };
    let bar_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("bar"),
    };
    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &foo1_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;

    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &foo2_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;
    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &bar_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;

    let res = kamu_api_server_client
        .graphql_api_call_ex(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                query search(
                  $query: String!,
                ) {
                    search {
                        query(query: $query) {
                            nodes {
                                ... on Dataset {
                                    alias
                                }
                            }
                        }
                    }
                }
                "#,
            ))
            .variables(async_graphql::Variables::from_value(
                async_graphql::value!({
                    "query": "foo",
                }),
            )),
        )
        .await;

    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "search": {
                "query": {
                    "nodes": [{
                        "alias": foo1_alias
                    }, {
                        "alias": foo2_alias
                    }],
                }
            }
        }),
        res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_dataset_by_account_substring(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_kamu().await;
    let bar_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("bar"),
    };
    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &bar_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;

    kamu_api_server_client.auth().logout();
    kamu_api_server_client.auth().login_as_e2e_user().await;

    let foo1_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("foo1"),
    };
    let foo2_alias = odf::DatasetAlias {
        account_name: Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        dataset_name: odf::DatasetName::new_unchecked("foo2"),
    };

    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &foo1_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;

    kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &foo2_alias,
            &odf::DatasetVisibility::Public,
        )
        .await;

    let res = kamu_api_server_client
        .graphql_api_call_ex(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                query search(
                  $query: String!,
                ) {
                    search {
                        query(query: $query) {
                            nodes {
                                ... on Dataset {
                                    alias
                                }
                            }
                        }
                    }
                }
                "#,
            ))
            .variables(async_graphql::Variables::from_value(
                async_graphql::value!({
                    "query": "e2e-",
                }),
            )),
        )
        .await;

    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "search": {
                "query": {
                    "nodes": [{
                        "alias": foo1_alias
                    }, {
                        "alias": foo2_alias
                    }],
                }
            }
        }),
        res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
