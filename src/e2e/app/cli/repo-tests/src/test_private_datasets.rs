// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_auth_rebac::AccountToDatasetRelation;
use kamu_cli_e2e_common::{
    AccountDataset,
    AccountRolesResponse,
    CreateDatasetResponse,
    DatasetCollaborationAccountRolesError,
    DatasetDependencies,
    DatasetDependency,
    FoundDatasetItem,
    GetDatasetVisibilityError,
    KamuApiServerClient,
    KamuApiServerClientExt,
    QueryError,
    QueryExecutionError,
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
          # Special:
          - accountName: admin
            isAdmin: true
            email: admin@example.com

          # By names:
          - accountName: alice
            email: alice@example.com
          - accountName: bob
            email: bob@example.com

          # By roles:
          - accountName: owner
            email: owner@example.com
          - accountName: not-owner
            email: not-owner@example.com
          - accountName: reader
            email: reader@example.com
          - accountName: editor
            email: editor@example.com
          - accountName: maintainer
            email: maintainer@example.com
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_datasets_have_correct_visibility_after_creation(anonymous: KamuApiServerClient) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("public").build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("private").build(),
            odf::DatasetVisibility::Private,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[&public_dataset_id, &private_dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    // Public
    for (tag, client) in [
        ("anonymous", &anonymous),
        ("not_owner", &not_owner),
        ("reader", &reader),
        ("editor", &editor),
        ("maintainer", &maintainer),
        ("owner", &owner),
        ("admin", &admin),
    ] {
        pretty_assertions::assert_eq!(
            Ok(odf::DatasetVisibility::Public),
            client.dataset().get_visibility(&public_dataset_id).await,
            "Tag: {}",
            tag,
        );
    }

    // Private
    let failure = Err(GetDatasetVisibilityError::NotFound) as Result<_, GetDatasetVisibilityError>;
    let success = Ok(odf::DatasetVisibility::Private);

    for (tag, client, expected_result) in [
        ("anonymous", &anonymous, &failure),
        ("not_owner", &not_owner, &failure),
        ("reader", &reader, &success),
        ("editor", &editor, &success),
        ("maintainer", &maintainer, &success),
        ("owner", &owner, &success),
        ("admin", &admin, &success),
    ] {
        pretty_assertions::assert_eq!(
            *expected_result,
            client.dataset().get_visibility(&private_dataset_id).await,
            "Tag: {}",
            tag,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_minimum_dataset_maintainer_can_change_dataset_visibility(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    let CreateDatasetResponse { dataset_id, .. } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("dataset").build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[&dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    // Unauthorized attempts to change visibility
    for (tag, client) in [
        ("anonymous", &anonymous),
        ("not_owner", &not_owner),
        ("reader", &reader),
        ("editor", &editor),
        // ("maintainer", &maintainer),
        // ("owner", &owner),
        // ("admin", &admin),
    ] {
        pretty_assertions::assert_eq!(
            Err(SetDatasetVisibilityError::Forbidden),
            client
                .dataset()
                .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
                .await,
            "Tag: {}",
            tag,
        );
    }

    // For an authorized user, change the visibility back and forth
    for (set_tag, set_client) in [
        ("maintainer", &maintainer),
        ("owner", &owner),
        ("admin", &admin),
    ] {
        // Making it private
        {
            pretty_assertions::assert_eq!(
                Ok(()),
                set_client
                    .dataset()
                    .set_visibility(&dataset_id, odf::DatasetVisibility::Private)
                    .await,
                "Tag: {}",
                set_tag,
            );

            let failure =
                Err(GetDatasetVisibilityError::NotFound) as Result<_, GetDatasetVisibilityError>;
            let success = Ok(odf::DatasetVisibility::Private);

            for (get_tag, get_client, expected_result) in [
                ("anonymous", &anonymous, &failure),
                ("not_owner", &not_owner, &failure),
                ("reader", &reader, &success),
                ("editor", &editor, &success),
                ("maintainer", &maintainer, &success),
                ("owner", &owner, &success),
                ("admin", &admin, &success),
            ] {
                pretty_assertions::assert_eq!(
                    *expected_result,
                    get_client.dataset().get_visibility(&dataset_id).await,
                    "Set tag: {}; Get tag: {}",
                    set_tag,
                    get_tag,
                );
            }
        }

        // Back to public
        {
            pretty_assertions::assert_eq!(
                Ok(()),
                set_client
                    .dataset()
                    .set_visibility(&dataset_id, odf::DatasetVisibility::Public)
                    .await,
                "Tag: {}",
                set_tag,
            );

            for (get_tag, get_client) in [
                ("anonymous", &anonymous),
                ("not_owner", &not_owner),
                ("reader", &reader),
                ("editor", &editor),
                ("maintainer", &maintainer),
                ("owner", &owner),
                ("admin", &admin),
            ] {
                pretty_assertions::assert_eq!(
                    Ok(odf::DatasetVisibility::Public),
                    get_client.dataset().get_visibility(&dataset_id).await,
                    "Set tag: {}; Get tag: {}",
                    set_tag,
                    get_tag,
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_only_to_authorized_users_in_the_search_results(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    // - owner has datasets:
    //     - private-dataset
    //     - public-dataset
    //     - private-will-not-be-found
    //     - public-will-not-be-found

    use odf::metadata::testing::alias;

    let searchable_private_dataset_alias = alias(&"owner", &"private-dataset");
    let CreateDatasetResponse {
        dataset_id: searchable_private_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(searchable_private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;

    let searchable_public_dataset_alias = alias(&"owner", &"public-dataset");
    let CreateDatasetResponse {
        dataset_id: searchable_public_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(searchable_public_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    let CreateDatasetResponse {
        dataset_id: public_dataset_id_will_not_be_found,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("public-will-not-be-found")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let CreateDatasetResponse {
        dataset_id: private_dataset_id_will_not_be_found,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("private-will-not-be-found")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[
            &searchable_private_dataset_id,
            &searchable_public_dataset_id,
            &public_dataset_id_will_not_be_found,
            &private_dataset_id_will_not_be_found,
        ],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    // Search

    let searchable_public_dataset = [FoundDatasetItem {
        id: searchable_public_dataset_id.clone(),
        alias: searchable_public_dataset_alias.clone(),
    }];
    let searchable_public_and_private_datasets = [
        FoundDatasetItem {
            id: searchable_private_dataset_id,
            alias: searchable_private_dataset_alias,
        },
        FoundDatasetItem {
            id: searchable_public_dataset_id,
            alias: searchable_public_dataset_alias,
        },
    ];

    for (tag, client, expected_result) in [
        ("anonymous", &anonymous, &searchable_public_dataset[..]),
        ("not_owner", &not_owner, &searchable_public_dataset[..]),
        (
            "reader",
            &reader,
            &searchable_public_and_private_datasets[..],
        ),
        (
            "editor",
            &editor,
            &searchable_public_and_private_datasets[..],
        ),
        (
            "maintainer",
            &maintainer,
            &searchable_public_and_private_datasets[..],
        ),
        ("owner", &owner, &searchable_public_and_private_datasets[..]),
        ("admin", &admin, &searchable_public_and_private_datasets[..]),
    ] {
        pretty_assertions::assert_eq!(
            expected_result,
            client.search().search("data").await,
            "Tag: {}",
            tag,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_in_queries_only_to_authorized_users(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("private-dataset")
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("public-dataset")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[&private_dataset_id, &public_dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    // public-dataset
    {
        let expected_result = Ok(indoc::indoc!(
            r#"
            public_count
            0"#
        )
        .to_string());

        for (tag, client) in [
            ("anonymous", &anonymous),
            ("not_owner", &not_owner),
            ("reader", &reader),
            ("editor", &editor),
            ("maintainer", &maintainer),
            ("owner", &owner),
            ("admin", &admin),
        ] {
            pretty_assertions::assert_eq!(
                expected_result,
                client
                    .odf_query()
                    .query(indoc::indoc!(
                        r#"
                        SELECT COUNT(*) AS public_count
                        FROM "owner/public-dataset";
                        "#
                    ))
                    .await,
                "Tag: {}",
                tag,
            );
        }
    }

    // private-dataset
    {
        let success = Ok(indoc::indoc!(
            r#"
            private_count
            0"#
        )
        .to_string());
        let failure = Err(QueryError::ExecutionError(QueryExecutionError {
            error_kind: "INVALID_SQL".to_string(),
            error_message: "table 'kamu.kamu.owner/private-dataset' not found".to_string(),
        }));

        for (tag, client, expected_result) in [
            ("anonymous", &anonymous, &failure),
            ("not_owner", &not_owner, &failure),
            ("reader", &reader, &success),
            ("editor", &editor, &success),
            ("maintainer", &maintainer, &success),
            ("owner", &owner, &success),
            ("admin", &admin, &success),
        ] {
            pretty_assertions::assert_eq!(
                *expected_result,
                client
                    .odf_query()
                    .query(indoc::indoc!(
                        r#"
                        SELECT COUNT(*) AS private_count
                        FROM "owner/private-dataset";
                        "#
                    ))
                    .await,
                "Tag: {}",
                tag,
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_is_available_only_to_authorized_users_in_a_users_dataset_list(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    use odf::metadata::testing::alias;

    let owner_name = odf::AccountName::new_unchecked("owner");

    let private_dataset_alias = alias(&owner_name, &"private-dataset");
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_dataset_alias = alias(&owner_name, &"public-dataset");
    let CreateDatasetResponse {
        dataset_id: public_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[&private_dataset_id, &public_dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    let only_public_dataset = [AccountDataset {
        id: public_dataset_id.clone(),
        alias: public_dataset_alias.clone(),
    }];
    let public_and_private_datasets = [
        AccountDataset {
            id: private_dataset_id,
            alias: private_dataset_alias,
        },
        AccountDataset {
            id: public_dataset_id,
            alias: public_dataset_alias,
        },
    ];

    for (tag, client, expected_result) in [
        ("anonymous", &anonymous, &only_public_dataset[..]),
        ("not_owner", &not_owner, &only_public_dataset[..]),
        ("reader", &reader, &public_and_private_datasets[..]),
        ("editor", &editor, &public_and_private_datasets[..]),
        ("maintainer", &maintainer, &public_and_private_datasets[..]),
        ("owner", &owner, &public_and_private_datasets[..]),
        ("admin", &admin, &public_and_private_datasets[..]),
    ] {
        pretty_assertions::assert_eq!(
            expected_result,
            client.dataset().by_account_name(&owner_name).await,
            "Tag: {}",
            tag,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_as_a_downstream_dependency_is_visible_only_to_authorized_users(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    //                                     ┌──────────────────────────────────┐
    //                                 ┌───┤ owner/private-derivative-dataset │
    // ┌───────────────────────────┐   │   └──────────────────────────────────┘
    // │ owner/public-root-dataset ├◄──┤
    // └───────────────────────────┘   │   ┌──────────────────────────────────┐
    //                                 └───┤ owner/public-derivative-dataset  │
    //                                     └──────────────────────────────────┘
    //
    use odf::metadata::testing::alias;

    let public_root_dataset_alias = alias(&"owner", &"public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: public_root_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_derivative_dataset_alias = alias(&"owner", &"private-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: private_derivative_dataset_id,
        ..
    } = owner
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
    let public_derivative_dataset_alias = alias(&"owner", &"public-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: public_derivative_dataset_id,
        ..
    } = owner
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

    authorize_users_by_roles(
        &owner,
        &[
            &public_root_dataset_id,
            &private_derivative_dataset_id,
            &public_derivative_dataset_id,
        ],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    let only_public_dataset = DatasetDependencies {
        upstream: vec![],
        downstream: vec![DatasetDependency::Resolved(ResolvedDatasetDependency {
            id: public_derivative_dataset_id.clone(),
            alias: public_derivative_dataset_alias.clone(),
        })],
    };
    let public_and_private_datasets = DatasetDependencies {
        upstream: vec![],
        downstream: vec![
            DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: private_derivative_dataset_id,
                alias: private_derivative_dataset_alias,
            }),
            DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: public_derivative_dataset_id,
                alias: public_derivative_dataset_alias,
            }),
        ],
    };

    for (tag, client, expected_result) in [
        ("anonymous", &anonymous, &only_public_dataset),
        ("not_owner", &not_owner, &only_public_dataset),
        ("reader", &reader, &public_and_private_datasets),
        ("editor", &editor, &public_and_private_datasets),
        ("maintainer", &maintainer, &public_and_private_datasets),
        ("owner", &owner, &public_and_private_datasets),
        ("admin", &admin, &public_and_private_datasets),
    ] {
        pretty_assertions::assert_eq!(
            *expected_result,
            client.dataset().dependencies(&public_root_dataset_id).await,
            "Tag: {}",
            tag,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_as_an_upstream_dependency_is_visible_only_to_authorized_users(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    // ┌────────────────────────────┐
    // │ owner/public-root-dataset  ├◄──┐
    // └────────────────────────────┘   │   ┌─────────────────────────────────┐
    //                                  ├───┤ owner/public-derivative-dataset │
    // ┌────────────────────────────┐   │   └─────────────────────────────────┘
    // │ owner/private-root-dataset ├◄──┘
    // └────────────────────────────┘
    //
    use odf::metadata::testing::alias;

    let public_root_dataset_alias = alias(&"owner", &"public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: public_root_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_root_dataset_alias = alias(&"owner", &"private-root-dataset");
    let CreateDatasetResponse {
        dataset_id: private_root_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_derivative_dataset_alias = alias(&"owner", &"public-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: public_derivative_dataset_id,
        ..
    } = owner
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

    authorize_users_by_roles(
        &owner,
        &[
            &public_root_dataset_id,
            &private_root_dataset_id,
            &public_derivative_dataset_id,
        ],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    let only_public_dataset = DatasetDependencies {
        upstream: vec![
            DatasetDependency::Unresolved(UnresolvedDatasetDependency {
                id: private_root_dataset_id.clone(),
                message: "Not Accessible".into(),
            }),
            DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: public_root_dataset_id.clone(),
                alias: public_root_dataset_alias.clone(),
            }),
        ],
        downstream: vec![],
    };
    let public_and_private_datasets = DatasetDependencies {
        upstream: vec![
            DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: private_root_dataset_id,
                alias: private_root_dataset_alias,
            }),
            DatasetDependency::Resolved(ResolvedDatasetDependency {
                id: public_root_dataset_id,
                alias: public_root_dataset_alias,
            }),
        ],
        downstream: vec![],
    };

    for (tag, client, expected_result) in [
        ("anonymous", &anonymous, &only_public_dataset),
        ("not_owner", &not_owner, &only_public_dataset),
        ("reader", &reader, &public_and_private_datasets),
        ("editor", &editor, &public_and_private_datasets),
        ("maintainer", &maintainer, &public_and_private_datasets),
        ("owner", &owner, &public_and_private_datasets),
        ("admin", &admin, &public_and_private_datasets),
    ] {
        pretty_assertions::assert_eq!(
            *expected_result,
            client
                .dataset()
                .dependencies(&public_derivative_dataset_id)
                .await,
            "Tag: {}",
            tag,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_private_dataset_can_only_be_pulled_by_authorized_users(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    use odf::metadata::testing::alias;

    let private_dataset_alias = alias(&"owner", &"private-dataset");
    let CreateDatasetResponse {
        dataset_id: private_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;

    authorize_users_by_roles(
        &owner,
        &[&private_dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    let node_url = owner.get_base_url();

    // TODO: PERF: Parallel execution?

    let sitp_private_dataset_url = owner.dataset().get_endpoint(&private_dataset_alias);
    let sitp_private_dataset_ref: odf::DatasetRefAny = sitp_private_dataset_url.clone().into();

    let smtp_private_dataset_url = owner.dataset().get_endpoint(&private_dataset_alias);
    let smtp_private_dataset_ref: odf::DatasetRefAny = smtp_private_dataset_url.clone().into();

    enum ExpectedPullResult {
        Success,
        Failure,
    }

    for (private_dataset_ref, private_dataset_url) in [
        (&sitp_private_dataset_ref, &sitp_private_dataset_url),
        (&smtp_private_dataset_ref, &smtp_private_dataset_url),
    ] {
        for (maybe_account_name, expected_result) in [
            (None /* anonymous */, ExpectedPullResult::Failure),
            (Some("not-owner"), ExpectedPullResult::Failure),
            (Some("reader"), ExpectedPullResult::Success),
            (Some("editor"), ExpectedPullResult::Success),
            (Some("maintainer"), ExpectedPullResult::Success),
            (Some("owner"), ExpectedPullResult::Success),
            (Some("admin"), ExpectedPullResult::Success),
        ] {
            let workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            if let Some(account_name) = maybe_account_name {
                let password_same_as_account_name = account_name;
                workspace
                    .login_to_node(node_url, account_name, password_same_as_account_name)
                    .await;
            }

            match expected_result {
                ExpectedPullResult::Success => {
                    workspace
                        .assert_success_command_execution(
                            ["pull", format!("{private_dataset_ref}").as_str()],
                            None,
                            Some([r#"1 dataset\(s\) updated"#]),
                        )
                        .await;
                }
                ExpectedPullResult::Failure => {
                    workspace
                        .assert_failure_command_execution(
                            ["pull", format!("{private_dataset_ref}").as_str()],
                            None,
                            Some([
                                r#"Failed to update 1 dataset\(s\)"#,
                                format!(
                                    r#"Failed to sync .*\/{0} from {1}: Dataset {1}/ not found"#,
                                    private_dataset_alias.dataset_name.as_str(),
                                    regex::escape(private_dataset_url.as_str())
                                )
                                .as_str(),
                            ]),
                        )
                        .await;
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_a_public_derivative_dataset_that_has_a_private_dependency_can_be_pulled_by_anyone(
    anonymous: KamuApiServerClient,
) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    // ┌────────────────────────────┐
    // │ owner/public-root-dataset  ├◄──┐
    // └────────────────────────────┘   │   ┌─────────────────────────────────┐
    //                                  ├───┤ owner/public-derivative-dataset │
    // ┌────────────────────────────┐   │   └─────────────────────────────────┘
    // │ owner/private-root-dataset ├◄──┘
    // └────────────────────────────┘
    //
    use odf::metadata::testing::alias;

    let public_root_dataset_alias = alias(&"owner", &"public-root-dataset");
    let CreateDatasetResponse {
        dataset_id: _public_root_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(public_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    let private_root_dataset_alias = alias(&"owner", &"private-root-dataset");
    let CreateDatasetResponse {
        dataset_id: _private_root_dataset_id,
        ..
    } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name(private_root_dataset_alias.dataset_name.as_str())
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;
    let public_derivative_dataset_alias = alias(&"owner", &"public-derivative-dataset");
    let CreateDatasetResponse {
        dataset_id: private_derivative_dataset_id,
        ..
    } = owner
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

    authorize_users_by_roles(
        &owner,
        &[&private_derivative_dataset_id],
        &mut reader,
        &mut editor,
        &mut maintainer,
    )
    .await;

    let node_url = owner.get_base_url();

    // TODO: PERF: Parallel execution?

    let sitp_public_derivative_dataset_url = owner
        .dataset()
        .get_endpoint(&public_derivative_dataset_alias);
    let sitp_public_derivative_dataset_ref: odf::DatasetRefAny =
        sitp_public_derivative_dataset_url.clone().into();

    let smtp_public_derivative_dataset_url = owner
        .dataset()
        .get_endpoint(&public_derivative_dataset_alias);
    let smtp_public_derivative_dataset_ref: odf::DatasetRefAny =
        smtp_public_derivative_dataset_url.clone().into();

    for (public_derivative_dataset_ref, private_dataset_url) in [
        (
            &sitp_public_derivative_dataset_ref,
            &sitp_public_derivative_dataset_url,
        ),
        (
            &smtp_public_derivative_dataset_ref,
            &smtp_public_derivative_dataset_url,
        ),
    ] {
        for maybe_account_name in [
            None, /* anonymous */
            Some("not-owner"),
            Some("reader"),
            Some("editor"),
            Some("maintainer"),
            Some("owner"),
            Some("admin"),
        ] {
            let workspace = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

            if let Some(account_name) = maybe_account_name {
                let password_same_as_account_name = account_name;
                workspace
                    .login_to_node(node_url, account_name, password_same_as_account_name)
                    .await;
            }

            workspace
                .assert_success_command_execution(
                    ["pull", format!("{public_derivative_dataset_ref}").as_str()],
                    None,
                    Some([r#"1 dataset\(s\) updated"#]),
                )
                .await;
        }
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

    use odf::metadata::testing::alias;

    let alice_public_root_dataset_alias = alias(&alice, &"public-root-dataset");
    let alice_private_root_dataset_alias = alias(&alice, &"private-root-dataset");
    let alice_public_derivative_dataset_alias = alias(&bob, &"public-derivative-dataset-1");
    let bob_public_derivative_dataset_alias = alias(&bob, &"public-derivative-dataset-2");
    let admin_public_derivative_dataset_alias = alias(&bob, &"public-derivative-dataset-3");

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
            r#"Failed to pull bob/public-derivative-dataset-2: Dataset not found: alice/private-root-dataset"#
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

pub async fn test_minimum_dataset_maintainer_can_access_roles(anonymous: KamuApiServerClient) {
    let [not_owner, mut reader, mut editor, mut maintainer, owner, admin] = make_logged_clients(
        &anonymous,
        [
            "not-owner",
            "reader",
            "editor",
            "maintainer",
            "owner",
            "admin",
        ],
    )
    .await;

    let CreateDatasetResponse { dataset_id, .. } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("dataset").build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    // No roles granted
    {
        let success = Ok(AccountRolesResponse::new(Vec::new()));
        let failure = Err(DatasetCollaborationAccountRolesError::DatasetNotFound);

        for (tag, client, expected_result) in [
            // failure
            ("anonymous", &anonymous, &failure),
            ("not_owner", &not_owner, &failure),
            ("reader", &reader, &failure),
            ("editor", &editor, &failure),
            ("maintainer", &maintainer, &failure),
            // success
            ("owner", &owner, &success),
            ("admin", &admin, &success),
        ] {
            pretty_assertions::assert_eq!(
                *expected_result,
                client
                    .dataset()
                    .collaboration()
                    .account_roles(&dataset_id)
                    .await,
                "Tag: {}",
                tag
            );
        }
    }

    // Grant roles
    {
        use AccountToDatasetRelation as R;

        for (tag, role_granting_client, target_account_id, role) in [
            (
                "admin to maintainer",
                &admin,
                maintainer.auth().logged_account_id(),
                R::Maintainer,
            ),
            (
                "owner to editor",
                &owner,
                editor.auth().logged_account_id(),
                R::Editor,
            ),
            (
                "maintainer to reader",
                &maintainer,
                reader.auth().logged_account_id(),
                R::Reader,
            ),
        ] {
            pretty_assertions::assert_eq!(
                Ok(()),
                role_granting_client
                    .dataset()
                    .collaboration()
                    .set_role(&dataset_id, &target_account_id, role)
                    .await,
                "Tag: {}",
                tag
            );
        }
    }

    // Validate granted roles
    {
        use odf::metadata::testing::account_name as name;
        use AccountToDatasetRelation as R;

        let success = Ok(vec![
            (name(&"editor"), R::Editor),
            (name(&"maintainer"), R::Maintainer),
            (name(&"reader"), R::Reader),
        ]);
        let not_found_failure = Err(DatasetCollaborationAccountRolesError::DatasetNotFound);
        let access_failure = Err(DatasetCollaborationAccountRolesError::Access);

        for (tag, client, expected_result) in [
            // not_found_failure
            ("anonymous", &anonymous, &not_found_failure),
            ("not_owner", &not_owner, &not_found_failure),
            // access_failure
            ("reader", &reader, &access_failure),
            ("editor", &editor, &access_failure),
            // success
            ("maintainer", &maintainer, &success),
            ("owner", &owner, &success),
            ("admin", &admin, &success),
        ] {
            pretty_assertions::assert_eq!(
                *expected_result,
                client
                    .dataset()
                    .collaboration()
                    .account_roles(&dataset_id)
                    .await
                    .map(|r| r
                        .accounts_with_roles
                        .into_iter()
                        .map(|a| (a.account_name, a.role))
                        .collect::<Vec<_>>()),
                "Tag: {}",
                tag
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_granted_role_is_correctly_replaced_by_another_one(
    anonymous: KamuApiServerClient,
) {
    let [alice, mut bob] = make_logged_clients(&anonymous, ["alice", "bob"]).await;

    let CreateDatasetResponse { dataset_id, .. } = alice
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("dataset").build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    use odf::metadata::testing::account_name as name;
    use AccountToDatasetRelation as R;

    // Bob is allowed to read
    {
        pretty_assertions::assert_eq!(
            Ok(()),
            alice
                .dataset()
                .collaboration()
                .set_role(&dataset_id, &bob.auth().logged_account_id(), R::Reader)
                .await
        );

        pretty_assertions::assert_eq!(
            Ok(vec![(name(&"bob"), R::Reader)]),
            alice
                .dataset()
                .collaboration()
                .account_roles(&dataset_id)
                .await
                .map(|r| r
                    .accounts_with_roles
                    .into_iter()
                    .map(|a| (a.account_name, a.role))
                    .collect::<Vec<_>>()),
        );
    }

    // Bob is allowed to write
    {
        pretty_assertions::assert_eq!(
            Ok(()),
            alice
                .dataset()
                .collaboration()
                .set_role(&dataset_id, &bob.auth().logged_account_id(), R::Editor)
                .await
        );

        pretty_assertions::assert_eq!(
            Ok(vec![(name(&"bob"), R::Editor)]),
            alice
                .dataset()
                .collaboration()
                .account_roles(&dataset_id)
                .await
                .map(|r| r
                    .accounts_with_roles
                    .into_iter()
                    .map(|a| (a.account_name, a.role))
                    .collect::<Vec<_>>()),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_granted_role_is_correctly_revoked(anonymous: KamuApiServerClient) {
    let [mut reader, mut editor, mut maintainer, owner] =
        make_logged_clients(&anonymous, ["reader", "editor", "maintainer", "owner"]).await;

    let CreateDatasetResponse { dataset_id, .. } = owner
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot().name("dataset").build(),
            odf::DatasetVisibility::Public,
        )
        .await;

    use odf::metadata::testing::account_name as name;
    use AccountToDatasetRelation as R;

    // Initial
    {
        authorize_users_by_roles(
            &owner,
            &[&dataset_id],
            &mut reader,
            &mut editor,
            &mut maintainer,
        )
        .await;

        pretty_assertions::assert_eq!(
            Ok(vec![
                (name(&"editor"), R::Editor),
                (name(&"maintainer"), R::Maintainer),
                (name(&"reader"), R::Reader),
            ]),
            owner
                .dataset()
                .collaboration()
                .account_roles(&dataset_id)
                .await
                .map(|r| r
                    .accounts_with_roles
                    .into_iter()
                    .map(|a| (a.account_name, a.role))
                    .collect::<Vec<_>>()),
        );
    }

    // Without reader
    {
        pretty_assertions::assert_eq!(
            Ok(()),
            owner
                .dataset()
                .collaboration()
                .unset_role(&dataset_id, &[&reader.auth().logged_account_id()],)
                .await
        );
        pretty_assertions::assert_eq!(
            Ok(vec![
                (name(&"editor"), R::Editor),
                (name(&"maintainer"), R::Maintainer),
            ]),
            owner
                .dataset()
                .collaboration()
                .account_roles(&dataset_id)
                .await
                .map(|r| r
                    .accounts_with_roles
                    .into_iter()
                    .map(|a| (a.account_name, a.role))
                    .collect::<Vec<_>>()),
        );
    }

    // Without editor + maintainer
    {
        pretty_assertions::assert_eq!(
            Ok(()),
            owner
                .dataset()
                .collaboration()
                .unset_role(
                    &dataset_id,
                    &[
                        &editor.auth().logged_account_id(),
                        &maintainer.auth().logged_account_id()
                    ],
                )
                .await
        );
        pretty_assertions::assert_eq!(
            Ok(Vec::new()),
            owner
                .dataset()
                .collaboration()
                .account_roles(&dataset_id)
                .await
                .map(|r| r
                    .accounts_with_roles
                    .into_iter()
                    .map(|a| (a.account_name, a.role))
                    .collect::<Vec<_>>()),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn make_logged_client(
    anonymous: &KamuApiServerClient,
    account_name: &str,
) -> KamuApiServerClient {
    let mut new_client = anonymous.clone();
    let password_same_as_account_name = account_name;

    new_client
        .auth()
        .login_with_password(account_name, password_same_as_account_name)
        .await;

    new_client
}

async fn make_logged_clients<const N: usize>(
    anonymous: &KamuApiServerClient,
    account_names: [&str; N],
) -> [KamuApiServerClient; N] {
    use std::mem::MaybeUninit;

    let mut logged_clients: [MaybeUninit<KamuApiServerClient>; N] = MaybeUninit::uninit_array();

    for (i, account_name) in account_names.iter().enumerate() {
        let logged_client = make_logged_client(anonymous, account_name).await;
        logged_clients[i].write(logged_client);
    }

    unsafe { logged_clients.map(|item| item.assume_init()) }
}

async fn authorize_users_by_roles(
    owner: &KamuApiServerClient,
    dataset_ids: &[&odf::DatasetID],
    reader: &mut KamuApiServerClient,
    editor: &mut KamuApiServerClient,
    maintainer: &mut KamuApiServerClient,
) {
    use AccountToDatasetRelation as R;

    for dataset_id in dataset_ids {
        for (tag, target_account_id, role) in [
            ("reader", reader.auth().logged_account_id(), R::Reader),
            ("editor", editor.auth().logged_account_id(), R::Editor),
            (
                "maintainer",
                maintainer.auth().logged_account_id(),
                R::Maintainer,
            ),
        ] {
            pretty_assertions::assert_eq!(
                Ok(()),
                owner
                    .dataset()
                    .collaboration()
                    .set_role(dataset_id, &target_account_id, role)
                    .await,
                "Tag: {}, dataset_id: {}",
                tag,
                dataset_id.as_did_str().to_stack_string()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
