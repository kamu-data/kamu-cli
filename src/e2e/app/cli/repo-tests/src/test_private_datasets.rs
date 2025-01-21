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
use odf::metadata::testing::MetadataFactory;

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
    let private_dataset_alias = test_utils::odf::alias("alice", "private-dataset");
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
    let public_dataset_alias = test_utils::odf::alias("alice", "public-dataset");
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

    let private_dataset_alias = test_utils::odf::alias(owner_name.as_str(), "private-dataset");
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
    let public_dataset_alias = test_utils::odf::alias(owner_name.as_str(), "public-dataset");
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

    let public_root_dataset_alias = test_utils::odf::alias("alice", "public-root-dataset");
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
        test_utils::odf::alias("alice", "private-derivative-dataset");
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
        test_utils::odf::alias("alice", "public-derivative-dataset");
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

    let public_root_dataset_alias = test_utils::odf::alias("alice", "public-root-dataset");
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
    let private_root_dataset_alias = test_utils::odf::alias("alice", "private-root-dataset");
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
    let derivative_dataset_alias = test_utils::odf::alias("alice", "private-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: derivative_dataset_id,
        ..
    } = owner_client
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(derivative_dataset_alias.dataset_name.as_str())
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
            .dependencies(&derivative_dataset_id)
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
            .dependencies(&derivative_dataset_id)
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
            .dependencies(&derivative_dataset_id)
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
            .dependencies(&derivative_dataset_id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
