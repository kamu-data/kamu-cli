// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{
    AccountDataset,
    CreateDatasetResponse,
    DatasetDependencies,
    DatasetDependency,
    FoundDatasetItem,
    GetDatasetVisibilityError,
    KamuApiServerClient,
    KamuApiServerClientExt,
    QueryError,
    ResolvedDatasetDependency,
    SetDatasetVisibilityError,
    UnresolvedDatasetDependency,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
};
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};
use kamu_cli_puppet::KamuCliPuppet;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      users:
        predefined:
          - accountName: admin
            isAdmin: true
            email: admin@example.com
          - accountName: alice
            email: alice@example.com
          - accountName: bob
            email: bob@example.com
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_only_the_dataset_owner_or_admin_can_change_its_visibility(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ dataset
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    owner_client
        .auth()
        .login_with_password("alice", "alice")
        .await;
    let CreateDatasetResponse { dataset_id, .. } = owner_client
        .dataset()
        .create_dataset_with_visibility(
            &DATASET_ROOT_PLAYER_SCORES_SNAPSHOT,
            odf::DatasetVisibility::Public,
        )
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    {
        assert_matches!(
            anonymous_client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
                .await,
            Err(SetDatasetVisibilityError::ForbiddenAnonymous)
        );

        assert_matches!(
            anonymous_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            not_owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
    }
    {
        assert_matches!(
            not_owner_client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
                .await,
            Err(SetDatasetVisibilityError::ForbiddenOnlyOwner)
        );

        assert_matches!(
            anonymous_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            not_owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
    }
    {
        assert_matches!(
            owner_client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
                .await,
            Ok(_)
        );

        assert_matches!(
            anonymous_client.dataset().get_visibility(&dataset_id).await,
            Err(GetDatasetVisibilityError::NotFound)
        );
        assert_matches!(
            owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Private)
        );
        assert_matches!(
            not_owner_client.dataset().get_visibility(&dataset_id).await,
            Err(GetDatasetVisibilityError::NotFound)
        );
        assert_matches!(
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Private)
        );
    }
    {
        assert_matches!(
            admin_client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Public)
                .await,
            Ok(_)
        );

        assert_matches!(
            anonymous_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            not_owner_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
        assert_matches!(
            admin_client.dataset().get_visibility(&dataset_id).await,
            Ok(odf::DatasetVisibility::Public)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_only_to_the_owner_or_admin_in_the_search_results(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    owner_client
        .auth()
        .login_with_password("alice", "alice")
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    // - alice (owner) has datasets:
    //     - private-dataset
    //     - public-dataset
    //     - private-will-not-be-found
    //     - public-will-not-be-found
    let private_dataset_alias = odf::metadata::testing::alias("alice", "private-dataset");
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_dataset_alias = odf::metadata::testing::alias("alice", "public-dataset");
    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("public-will-not-be-found")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("private-will-not-be-found")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    pretty_assertions::assert_eq!(
        [FoundDatasetItem {
            id: public_dataset_id.clone(),
            alias: public_dataset_alias.clone()
        }],
        *anonymous_client.search().search("data").await
    );
    pretty_assertions::assert_eq!(
        [FoundDatasetItem {
            id: public_dataset_id.clone(),
            alias: public_dataset_alias.clone()
        }],
        *not_owner_client.search().search("data").await
    );
    pretty_assertions::assert_eq!(
        [
            FoundDatasetItem {
                id: private_dataset_id.clone(),
                alias: private_dataset_alias.clone()
            },
            FoundDatasetItem {
                id: public_dataset_id.clone(),
                alias: public_dataset_alias.clone()
            }
        ],
        *owner_client.search().search("data").await
    );
    pretty_assertions::assert_eq!(
        [
            FoundDatasetItem {
                id: private_dataset_id,
                alias: private_dataset_alias
            },
            FoundDatasetItem {
                id: public_dataset_id,
                alias: public_dataset_alias
            }
        ],
        *owner_client.search().search("data").await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_in_queries_only_for_the_owner_or_admin(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    owner_client
        .auth()
        .login_with_password("alice", "alice")
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("private-dataset")
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("public-dataset")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    let sql_query_private_dataset = indoc::indoc!(
        r#"
        SELECT COUNT(*) AS private_count
        FROM "alice/private-dataset";
        "#
    );
    let expected_private_dataset_output = indoc::indoc!(
        r#"
        private_count
        0"#
    );

    assert_matches!(
        anonymous_client
            .odf_query()
            .query(sql_query_private_dataset)
            .await,
        Err(QueryError::ExecutionError(e))
            if e.error_kind == "INVALID_SQL"
                && e.error_message == "table 'kamu.kamu.alice/private-dataset' not found"
    );
    assert_matches!(
        not_owner_client
            .odf_query()
            .query(sql_query_private_dataset)
            .await,
        Err(QueryError::ExecutionError(e))
            if e.error_kind == "INVALID_SQL"
                && e.error_message == "table 'kamu.kamu.alice/private-dataset' not found"
    );
    assert_matches!(
        owner_client
            .odf_query()
            .query(sql_query_private_dataset)
            .await,
        Ok(result)
            if result == expected_private_dataset_output
    );
    assert_matches!(
        admin_client
            .odf_query()
            .query(sql_query_private_dataset)
            .await,
        Ok(result)
            if result == expected_private_dataset_output
    );

    let sql_query_public_dataset = indoc::indoc!(
        r#"
        SELECT COUNT(*) AS public_count
        FROM "alice/public-dataset";
        "#
    );
    let expected_public_dataset_output = indoc::indoc!(
        r#"
        public_count
        0"#
    );

    assert_matches!(
        anonymous_client
            .odf_query()
            .query(sql_query_public_dataset)
            .await,
        Ok(result)
            if result == expected_public_dataset_output
    );
    assert_matches!(
        not_owner_client
            .odf_query()
            .query(sql_query_public_dataset)
            .await,
        Ok(result)
            if result == expected_public_dataset_output
    );
    assert_matches!(
        owner_client
            .odf_query()
            .query(sql_query_public_dataset)
            .await,
        Ok(result)
            if result == expected_public_dataset_output
    );
    assert_matches!(
        admin_client
            .odf_query()
            .query(sql_query_public_dataset)
            .await,
        Ok(result)
            if result == expected_public_dataset_output
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_only_to_the_owner_or_admin_in_a_users_dataset_list(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    let owner_name = odf::AccountName::new_unchecked("alice");
    owner_client
        .auth()
        .login_with_password(owner_name.as_str(), "alice")
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    let private_dataset_alias =
        odf::metadata::testing::alias(owner_name.as_str(), "private-dataset");
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_dataset_alias = odf::metadata::testing::alias(owner_name.as_str(), "public-dataset");
    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    pretty_assertions::assert_eq!(
        [AccountDataset {
            id: public_dataset_id.clone(),
            alias: public_dataset_alias.clone()
        }],
        *not_owner_client
            .dataset()
            .by_account_name(&owner_name)
            .await
    );
    pretty_assertions::assert_eq!(
        [
            AccountDataset {
                id: private_dataset_id.clone(),
                alias: private_dataset_alias.clone()
            },
            AccountDataset {
                id: public_dataset_id.clone(),
                alias: public_dataset_alias.clone()
            }
        ],
        *owner_client.dataset().by_account_name(&owner_name).await
    );
    pretty_assertions::assert_eq!(
        [AccountDataset {
            id: public_dataset_id.clone(),
            alias: public_dataset_alias.clone()
        }],
        *anonymous_client
            .dataset()
            .by_account_name(&owner_name)
            .await
    );
    pretty_assertions::assert_eq!(
        [
            AccountDataset {
                id: private_dataset_id,
                alias: private_dataset_alias
            },
            AccountDataset {
                id: public_dataset_id,
                alias: public_dataset_alias
            }
        ],
        *owner_client.dataset().by_account_name(&owner_name).await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_as_a_downstream_dependency_is_visible_only_to_the_owner_or_admin(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    let owner_name = odf::AccountName::new_unchecked("alice");
    owner_client
        .auth()
        .login_with_password(owner_name.as_str(), "alice")
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    //                                     ┌──────────────────────────────────┐
    //                                 ┌───┤ alice/private-derivative-dataset │
    // ┌───────────────────────────┐   │   └──────────────────────────────────┘
    // │ alice/public-root-dataset ├◄──┤
    // └───────────────────────────┘   │   ┌──────────────────────────────────┐
    //                                 └───┤ alice/public-derivative-dataset  │
    //                                     └──────────────────────────────────┘
    //
    let public_root_dataset_alias = odf::metadata::testing::alias("alice", "public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: public_root_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_derivative_dataset_alias =
        odf::metadata::testing::alias("alice", "private-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: private_derivative_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_derivative_dataset_alias.dataset_name.as_str())
                .kind(odf::DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(vec![public_root_dataset_alias.clone()])
                        .build(),
                )
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_derivative_dataset_alias =
        odf::metadata::testing::alias("alice", "public-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: public_derivative_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_derivative_dataset_alias.dataset_name.as_str())
                .kind(odf::DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(vec![public_root_dataset_alias])
                        .build(),
                )
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![],
            downstream: vec![DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: public_derivative_dataset_id.clone(),
                alias: public_derivative_dataset_alias.clone()
            })]
        },
        anonymous_client
            .dataset()
            .dependencies(&public_root_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![],
            downstream: vec![DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: public_derivative_dataset_id.clone(),
                alias: public_derivative_dataset_alias.clone()
            })]
        },
        not_owner_client
            .dataset()
            .dependencies(&public_root_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![],
            downstream: vec![
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: private_derivative_dataset_id.clone(),
                    alias: private_derivative_dataset_alias.clone()
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_derivative_dataset_id.clone(),
                    alias: public_derivative_dataset_alias.clone()
                }),
            ]
        },
        owner_client
            .dataset()
            .dependencies(&public_root_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![],
            downstream: vec![
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: private_derivative_dataset_id.clone(),
                    alias: private_derivative_dataset_alias.clone()
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_derivative_dataset_id.clone(),
                    alias: public_derivative_dataset_alias.clone()
                }),
            ]
        },
        admin_client
            .dataset()
            .dependencies(&public_root_dataset_id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_as_an_upstream_dependency_is_visible_only_to_the_owner_or_admin(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let anonymous_client = kamu_api_server_client.clone();

    let mut owner_client = kamu_api_server_client.clone();
    let owner_name = odf::AccountName::new_unchecked("alice");
    owner_client
        .auth()
        .login_with_password(owner_name.as_str(), "alice")
        .await;

    let mut not_owner_client = kamu_api_server_client.clone();
    not_owner_client
        .auth()
        .login_with_password("bob", "bob")
        .await;

    let mut admin_client = kamu_api_server_client;
    admin_client
        .auth()
        .login_with_password("admin", "admin")
        .await;

    // ┌────────────────────────────┐
    // │ alice/public-root-dataset  ├◄──┐
    // └────────────────────────────┘   │   ┌─────────────────────────────────┐
    //                                  ├───┤ alice/public-derivative-dataset │
    // ┌────────────────────────────┐   │   └─────────────────────────────────┘
    // │ alice/private-root-dataset ├◄──┘
    // └────────────────────────────┘
    //
    let public_root_dataset_alias = odf::metadata::testing::alias("alice", "public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: public_root_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_root_dataset_alias = odf::metadata::testing::alias("alice", "private-root-dataset");
    let CreateDatasetResponse {
        dataset_id: private_root_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_derivative_dataset_alias =
        odf::metadata::testing::alias("alice", "public-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: public_derivative_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_derivative_dataset_alias.dataset_name.as_str())
                .kind(odf::DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(vec![
                            public_root_dataset_alias.clone(),
                            private_root_dataset_alias.clone(),
                        ])
                        .build(),
                )
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![
                DatasetDependency::Unresolved(UnresolvedDatasetDependency {
                    id: private_root_dataset_id.clone(),
                    message: "Not Accessible".into()
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_root_dataset_id.clone(),
                    alias: public_root_dataset_alias.clone()
                })
            ],
            downstream: vec![]
        },
        anonymous_client
            .dataset()
            .dependencies(&public_derivative_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![
                DatasetDependency::Unresolved(UnresolvedDatasetDependency {
                    id: private_root_dataset_id.clone(),
                    message: "Not Accessible".into()
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_root_dataset_id.clone(),
                    alias: public_root_dataset_alias.clone()
                })
            ],
            downstream: vec![]
        },
        not_owner_client
            .dataset()
            .dependencies(&public_derivative_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: private_root_dataset_id.clone(),
                    alias: private_root_dataset_alias.clone()
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_root_dataset_id.clone(),
                    alias: public_root_dataset_alias.clone()
                })
            ],
            downstream: vec![]
        },
        owner_client
            .dataset()
            .dependencies(&public_derivative_dataset_id)
            .await
    );
    pretty_assertions::assert_eq!(
        DatasetDependencies {
            upstream: vec![
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: private_root_dataset_id,
                    alias: private_root_dataset_alias
                }),
                DatasetDependency::Resolved(ResolvedDatasetDependency {
                    id: public_root_dataset_id,
                    alias: public_root_dataset_alias
                })
            ],
            downstream: vec![]
        },
        admin_client
            .dataset()
            .dependencies(&public_derivative_dataset_id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_can_only_be_pulled_by_the_owner_or_admin(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let mut owner_client = kamu_api_server_client;
    let owner_name = odf::AccountName::new_unchecked("alice");
    owner_client
        .auth()
        .login_with_password(owner_name.as_str(), "alice")
        .await;

    let private_dataset_alias = odf::metadata::testing::alias("alice", "private-dataset");
    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;

    let node_url = owner_client.get_base_url();

    // SiTP
    {
        let private_dataset_url = owner_client.dataset().get_endpoint(&private_dataset_alias);
        let private_dataset_ref: odf::DatasetRefAny = private_dataset_url.clone().into();

        {
            let anonymous_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            anonymous_workspace
                .assert_failure_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([
                        r#"Failed to update 1 dataset\(s\)"#,
                        format!(
                            r#"Failed to sync .*\/{0} from {1}: odf::Dataset {1}/ not found"#,
                            private_dataset_alias.dataset_name.as_str(),
                            regex::escape(private_dataset_url.as_str())
                        )
                        .as_str(),
                    ]),
                )
                .await;
        }
        {
            let bob_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            bob_workspace.login_to_node(node_url, "bob", "bob").await;

            bob_workspace
                .assert_failure_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([
                        r#"Failed to update 1 dataset\(s\)"#,
                        format!(
                            r#"Failed to sync .*\/{0} from {1}: odf::Dataset {1}/ not found"#,
                            private_dataset_alias.dataset_name.as_str(),
                            regex::escape(private_dataset_url.as_str())
                        )
                        .as_str(),
                    ]),
                )
                .await;
        }
        {
            let owner_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            owner_workspace
                .login_to_node(node_url, "alice", "alice")
                .await;

            owner_workspace
                .assert_success_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([r#"1 dataset\(s\) updated"#]),
                )
                .await;
        }
        {
            let admin_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            admin_workspace
                .login_to_node(node_url, "admin", "admin")
                .await;

            admin_workspace
                .assert_success_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([r#"1 dataset\(s\) updated"#]),
                )
                .await;
        }
    }
    // SmTP
    {
        let private_dataset_odf_url = owner_client
            .dataset()
            .get_odf_endpoint(&private_dataset_alias);
        let private_dataset_ref: odf::DatasetRefAny = private_dataset_odf_url.clone().into();

        {
            let anonymous_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            anonymous_workspace
                .assert_failure_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([
                        r#"Failed to update 1 dataset\(s\)"#,
                        format!(
                            r#"Failed to sync .*\/{0} from {1}: odf::Dataset {1}/ not found"#,
                            private_dataset_alias.dataset_name.as_str(),
                            regex::escape(private_dataset_odf_url.as_str())
                        )
                        .as_str(),
                    ]),
                )
                .await;
        }
        {
            let bob_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            bob_workspace.login_to_node(node_url, "bob", "bob").await;

            bob_workspace
                .assert_failure_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([
                        r#"Failed to update 1 dataset\(s\)"#,
                        format!(
                            r#"Failed to sync .*\/{0} from {1}: odf::Dataset {1}/ not found"#,
                            private_dataset_alias.dataset_name.as_str(),
                            regex::escape(private_dataset_odf_url.as_str())
                        )
                        .as_str(),
                    ]),
                )
                .await;
        }
        {
            let owner_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            owner_workspace
                .login_to_node(node_url, "alice", "alice")
                .await;

            owner_workspace
                .assert_success_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([r#"1 dataset\(s\) updated"#]),
                )
                .await;
        }
        {
            let admin_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            admin_workspace
                .login_to_node(node_url, "admin", "admin")
                .await;

            admin_workspace
                .assert_success_command_execution(
                    ["pull", format!("{private_dataset_ref}").as_str()],
                    None,
                    Some([r#"1 dataset\(s\) updated"#]),
                )
                .await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_public_derivative_dataset_that_has_a_private_dependency_can_be_pulled_by_anyone(
    kamu_api_server_client: KamuApiServerClient,
) {
    // 4 actors:
    // - anonymous
    // - alice (owner) w/ datasets
    // - bob (not owner)
    // - admin
    let mut owner_client = kamu_api_server_client;
    let owner_name = odf::AccountName::new_unchecked("alice");
    owner_client
        .auth()
        .login_with_password(owner_name.as_str(), "alice")
        .await;

    // ┌────────────────────────────┐
    // │ alice/public-root-dataset  ├◄──┐
    // └────────────────────────────┘   │   ┌─────────────────────────────────┐
    //                                  ├───┤ alice/public-derivative-dataset │
    // ┌────────────────────────────┐   │   └─────────────────────────────────┘
    // │ alice/private-root-dataset ├◄──┘
    // └────────────────────────────┘
    //
    let public_root_dataset_alias = odf::metadata::testing::alias("alice", "public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: _public_root_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_root_dataset_alias = odf::metadata::testing::alias("alice", "private-root-dataset");
    let CreateDatasetResponse {
        dataset_id: _private_root_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_derivative_dataset_alias =
        odf::metadata::testing::alias("alice", "public-derivative-dataset");
    owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_derivative_dataset_alias.dataset_name.as_str())
                .kind(odf::DatasetKind::Derivative)
                .push_event(
                    MetadataFactory::set_transform()
                        .inputs_from_refs(vec![
                            public_root_dataset_alias.clone(),
                            private_root_dataset_alias.clone(),
                        ])
                        .build(),
                )
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    let public_derivative_dataset_ref: odf::DatasetRefAny = owner_client
        .dataset()
        .get_odf_endpoint(&public_derivative_dataset_alias)
        .into();
    let node_url = owner_client.get_base_url();

    {
        let anonymous_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

        anonymous_workspace
            .assert_success_command_execution(
                ["pull", format!("{public_derivative_dataset_ref}").as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;
    }
    {
        let bob_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

        bob_workspace.login_to_node(node_url, "bob", "bob").await;

        bob_workspace
            .assert_success_command_execution(
                ["pull", format!("{public_derivative_dataset_ref}").as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;
    }
    {
        let owner_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

        owner_workspace
            .login_to_node(node_url, "alice", "alice")
            .await;

        owner_workspace
            .assert_success_command_execution(
                ["pull", format!("{public_derivative_dataset_ref}").as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;
    }
    {
        let admin_workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

        admin_workspace
            .login_to_node(node_url, "admin", "admin")
            .await;

        admin_workspace
            .assert_success_command_execution(
                ["pull", format!("{public_derivative_dataset_ref}").as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_dataset_that_has_a_private_dependency_can_only_be_pulled_in_workspace_by_the_owner_or_admin(
    mut kamu: KamuCliPuppet,
) {
    // 3 actors:
    // - alice
    // - bob
    // - admin

    //                                         ┌───────────────────────────────────┐
    //                                      ┌◄─┤ alice/public-derivative-dataset-1 │
    // ┌────────────────────────────┐       │  └───────────────────────────────────┘
    // │ alice/public-root-dataset  ├◄──┐   │
    // └────────────────────────────┘   │   │  ┌───────────────────────────────────┐
    //                                  ├─◄─┤◄─┤  bob/public-derivative-dataset-2  │
    // ┌────────────────────────────┐   │   │  └───────────────────────────────────┘
    // │ alice/private-root-dataset ├◄──┘   │
    // └────────────────────────────┘       │  ┌───────────────────────────────────┐
    //                                      └◄─┤ admin/public-derivative-dataset-3 │
    //                                         └───────────────────────────────────┘
    //
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");
    let admin = odf::AccountName::new_unchecked("admin");

    let alice_public_root_dataset_alias =
        odf::metadata::testing::alias(alice.as_str(), "public-root-dataset");
    let alice_private_root_dataset_alias =
        odf::metadata::testing::alias(alice.as_str(), "private-root-dataset");
    let alice_public_derivative_dataset_alias =
        odf::metadata::testing::alias(bob.as_str(), "public-derivative-dataset-1");
    let bob_public_derivative_dataset_alias =
        odf::metadata::testing::alias(bob.as_str(), "public-derivative-dataset-2");
    let admin_public_derivative_dataset_alias =
        odf::metadata::testing::alias(bob.as_str(), "public-derivative-dataset-3");

    // Alice

    kamu.set_account(Some(alice.clone()));

    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_public_root_dataset_alias.dataset_name.as_str())
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_private_root_dataset_alias.dataset_name.as_str())
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Private)
            .build(),
    )
    .await;
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_public_derivative_dataset_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![
                        alice_public_root_dataset_alias.clone(),
                        alice_private_root_dataset_alias.clone(),
                    ])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "pull",
            alice_public_derivative_dataset_alias.dataset_name.as_str(),
        ],
        None,
        Some([r#"1 dataset\(s\) up-to-date"#]),
    )
    .await;

    // Bob

    kamu.set_account(Some(bob));

    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(bob_public_derivative_dataset_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![
                        alice_public_root_dataset_alias.clone(),
                        alice_private_root_dataset_alias.clone(),
                    ])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.assert_failure_command_execution(
        [
            "pull",
            bob_public_derivative_dataset_alias.dataset_name.as_str(),
        ],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            r#"Failed to pull public-derivative-dataset-2: Dataset not found: alice/private-root-dataset"#
        ]),
    )
        .await;

    // Admin

    kamu.set_account(Some(admin));

    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(admin_public_derivative_dataset_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![
                        alice_public_root_dataset_alias,
                        alice_private_root_dataset_alias,
                    ])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "pull",
            admin_public_derivative_dataset_alias.dataset_name.as_str(),
        ],
        None,
        Some([r#"1 dataset\(s\) up-to-date"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
