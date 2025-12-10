// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use base64::Engine as _;
use indoc::indoc;
use kamu_accounts::LoggedAccount;
use kamu_core::*;
use kamu_molecule_domain::MoleculeCreateProjectUseCase;
use num_bigint::BigInt;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::test_gql_custom_molecule_v1::GraphQLMoleculeV1Harness;
use crate::utils::GraphQLQueryRequest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CREATE_PROJECT: &str = indoc!(
    r#"
    mutation (
        $ipnftSymbol: String!,
        $ipnftUid: String!,
        $ipnftAddress: String!,
        $ipnftTokenId: Int!,
    ) {
        molecule {
            v2 {
                createProject(
                    ipnftSymbol: $ipnftSymbol,
                    ipnftUid: $ipnftUid,
                    ipnftAddress: $ipnftAddress,
                    ipnftTokenId: $ipnftTokenId,
                ) {
                    isSuccess
                    message
                    __typename
                    ... on CreateProjectSuccess {
                        project {
                            account { id accountName }
                            ipnftUid
                            dataRoom { id alias }
                            announcements { id alias }
                        }
                    }
                }
            }
        }
    }
    "#
);

const DISABLE_PROJECT: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!) {
        molecule {
            v2 {
                disableProject(ipnftUid: $ipnftUid) {
                    project { __typename }
                }
            }
        }
    }
    "#
);

const ENABLE_PROJECT: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!) {
        molecule {
            v2 {
                enableProject(ipnftUid: $ipnftUid) {
                    project { __typename }
                }
            }
        }
    }
    "#
);

// TODO: find a way to output tags/categories
const LIST_GLOBAL_ACTIVITY_QUERY: &str = indoc!(
    r#"
    query ($filters: MoleculeProjectActivityFilters) {
      molecule {
        v2 {
          activity(filters: $filters) {
            nodes {
              ... on MoleculeActivityFileAddedV2 {
                __typename
                entry {
                  path
                  ref
                  changeBy
                }
              }
              ... on MoleculeActivityFileUpdatedV2 {
                __typename
                entry {
                  path
                  ref
                  changeBy
                }
              }
              ... on MoleculeActivityFileRemovedV2 {
                __typename
                entry {
                  path
                  ref
                  changeBy
                }
              }
              ... on MoleculeActivityAnnouncementV2 {
                __typename
                announcement {
                  id
                  headline
                  body
                  attachments
                  accessLevel
                  changeBy
                  categories
                  tags
                }
              }
            }
          }
        }
      }
    }
    "#
);
const LIST_PROJECT_ACTIVITY_QUERY: &str = indoc!(
    r#"
    query ($ipnftUid: String!, $filters: MoleculeProjectActivityFilters) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            activity(filters: $filters) {
              nodes {
                ... on MoleculeActivityFileAddedV2 {
                  __typename
                  entry {
                    path
                    ref
                    changeBy
                  }
                }
                ... on MoleculeActivityFileUpdatedV2 {
                  __typename
                  entry {
                    path
                    ref
                    changeBy
                  }
                }
                ... on MoleculeActivityFileRemovedV2 {
                  __typename
                  entry {
                    path
                    ref
                    changeBy
                  }
                }
                ... on MoleculeActivityAnnouncementV2 {
                  __typename
                  announcement {
                    id
                    headline
                    body
                    attachments
                    accessLevel
                    changeBy
                    categories
                    tags
                  }
                }
              }
            }
          }
        }
      }
    }
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_provision_project() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Setup `projects` dataset
    harness.create_projects_dataset().await;

    // List of projects is empty
    const LIST_PROJECTS: &str = indoc!(
        r#"
        query {
            molecule {
                v2 {
                    projects(page: 0, perPage: 100) {
                        nodes {
                            ipnftSymbol
                            ipnftUid
                        }
                    }
                }
            }
        }
        "#
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([]),
    );

    // Create first project
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "VITAFAST",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res = &res.data.into_json().unwrap()["molecule"]["v2"]["createProject"];
    let project_account_id = res["project"]["account"]["id"].as_str().unwrap();
    let project_account_name = res["project"]["account"]["accountName"].as_str().unwrap();
    let data_room_did = res["project"]["dataRoom"]["id"].as_str().unwrap();
    let announcements_did = res["project"]["announcements"]["id"].as_str().unwrap();
    assert_ne!(project_account_id, harness.molecule_account_id.to_string());
    assert_eq!(project_account_name, "molecule.vitafast");
    assert_eq!(
        *res,
        json!({
            "isSuccess": true,
            "message": "",
            "__typename": "CreateProjectSuccess",
            "project": {
                "account": {
                    "id": project_account_id,
                    "accountName": project_account_name,
                },
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "dataRoom": {
                    "id": data_room_did,
                    "alias": format!("{project_account_name}/data-room"),
                },
                "announcements": {
                    "id": announcements_did,
                    "alias": format!("{project_account_name}/announcements"),
                },
            },
        }),
    );

    // Read back the project entry by `ipnftUid``
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($ipnftUid: String!) {
                    molecule {
                        v2 {
                            project(ipnftUid: $ipnftUid) {
                                account { id accountName }
                                ipnftSymbol
                                ipnftUid
                                ipnftAddress
                                ipnftTokenId
                            }
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"],
        json!({
            "account": {
                "id": project_account_id,
                "accountName": project_account_name,
            },
            "ipnftSymbol": "vitafast",
            "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
            "ipnftTokenId": "9",
        }),
    );

    // Project appears in the list
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([{
            "ipnftSymbol": "vitafast",
            "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
        }]),
    );

    // Ensure errors on ipnftUid collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitaslow",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project vitafast (0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );

    // Ensure errors on ipnftSymbol collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_1",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "1",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project vitafast (0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );

    // Create another project
    // Ensure errors on ipnftUid collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitaslow",
                "ipnftUid": r#"0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_108494037067113761580099112583860151730516105403483528465874625006707409835912"#,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "108494037067113761580099112583860151730516105403483528465874625006707409835912",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
    );

    // Both projects appear in the list
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([
            {
                "ipnftSymbol": "vitafast",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            },
            {
                "ipnftSymbol": "vitaslow",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_108494037067113761580099112583860151730516105403483528465874625006707409835912",
            }
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_disable_enable_project() {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let molecule_subject = LoggedAccount {
        account_id: harness.molecule_account_id.clone(),
        account_name: "molecule".parse().unwrap(),
    };

    let create_project_uc = harness
        .catalog_authorized
        .get_one::<dyn MoleculeCreateProjectUseCase>()
        .unwrap();

    create_project_uc
        .execute(
            &molecule_subject,
            "VITAFAST".to_string(),
            ipnft_uid.to_string(),
            "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1".to_string(),
            BigInt::from(9u32),
        )
        .await
        .unwrap();

    let projects_dataset_alias = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked("molecule")),
        odf::DatasetName::new_unchecked("projects"),
    );

    let initial_chain_len = harness
        .projects_metadata_chain_len(&projects_dataset_alias)
        .await;

    async fn query_project(
        harness: &GraphQLMoleculeV1Harness,
        uid: &str,
    ) -> async_graphql::Response {
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query ($ipnftUid: String!) {
                    molecule {
                        v2 {
                            project(ipnftUid: $ipnftUid) {
                                ipnftUid
                                ipnftSymbol
                            }
                        }
                    }
                }
                "#
            ),
            async_graphql::Variables::from_json(json!({ "ipnftUid": uid })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
    }

    // Disable project multiple times to test idempotence
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(DISABLE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({ "ipnftUid": ipnft_uid })),
        ))
        .await;
    assert!(res.is_ok(), "{res:#?}");
    let disable_res = res.data.into_json().unwrap();
    assert_eq!(
        disable_res["molecule"]["v2"]["disableProject"]["project"],
        json!({ "__typename": "MoleculeProjectMutV2" }),
    );

    // Trying to disable disabled account will return error
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(DISABLE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({ "ipnftUid": ipnft_uid })),
        ))
        .await;
    let disable_error = res.errors[0].message.clone();
    let res_json = res.data.into_json().unwrap();
    assert!(res_json["molecule"]["v2"]["disableProject"].is_null());
    assert_eq!(disable_error, format!("Project {ipnft_uid} not found"));

    // Project is no longer visible in the listing
    let res = GraphQLQueryRequest::new(
        indoc!(
            r#"
            query {
                molecule {
                    v2 {
                        projects(page: 0, perPage: 100) {
                            nodes { ipnftUid }
                        }
                    }
                }
            }
            "#
        ),
        async_graphql::Variables::default(),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([]),
    );

    // Querying by UID returns None
    let res = query_project(&harness, ipnft_uid).await;
    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"],
        json!(null),
    );

    // Enable project multiple times to test idempotence
    for _ in 0..2 {
        let res = harness
            .execute_authorized_query(async_graphql::Request::new(ENABLE_PROJECT).variables(
                async_graphql::Variables::from_json(json!({ "ipnftUid": ipnft_uid })),
            ))
            .await;
        assert!(res.is_ok(), "{res:#?}");
        let enable_res = res.data.into_json().unwrap();
        assert_eq!(
            enable_res["molecule"]["v2"]["enableProject"]["project"],
            json!({ "__typename": "MoleculeProjectMutV2" }),
        );
    }

    let post_enable_chain_len = harness
        .projects_metadata_chain_len(&projects_dataset_alias)
        .await;
    // it should be two blocks longer than initial (one disable and one enable
    // operations)
    assert_eq!(post_enable_chain_len, initial_chain_len + 2);

    let res = GraphQLQueryRequest::new(
        indoc!(
            r#"
            query {
                molecule {
                    v2 {
                        projects(page: 0, perPage: 100) {
                            nodes { ipnftUid }
                        }
                    }
                }
            }
            "#
        ),
        async_graphql::Variables::default(),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([{
            "ipnftUid": ipnft_uid,
        }]),
    );

    let res = query_project(&harness, ipnft_uid).await;
    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"],
        json!({
            "ipnftUid": ipnft_uid,
            "ipnftSymbol": "vitafast",
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_disable_enable_project_errors() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_projects_dataset().await;

    let missing_uid = "foo";

    let res = GraphQLQueryRequest::new(
        DISABLE_PROJECT,
        async_graphql::Variables::from_json(json!({ "ipnftUid": missing_uid })),
    )
    .expect_error()
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    let disable_error = res.errors[0].message.clone();
    let res_json = res.data.into_json().unwrap();
    assert!(res_json["molecule"]["v2"]["disableProject"].is_null());
    assert_eq!(disable_error, format!("Project {missing_uid} not found"));

    let res = GraphQLQueryRequest::new(
        ENABLE_PROJECT,
        async_graphql::Variables::from_json(json!({ "ipnftUid": missing_uid })),
    )
    .expect_error()
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    let enable_error = res.errors[0].message.clone();
    let res_json = res.data.into_json().unwrap();
    assert!(res_json["molecule"]["v2"]["enableProject"].is_null());
    assert_eq!(
        enable_error,
        format!("No historical entries for project {missing_uid}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_cannot_recreate_disabled_project_with_same_symbol() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_projects_dataset().await;

    let original_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    // Create project
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": original_uid,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;
    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
    );

    // Disable project
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(DISABLE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({ "ipnftUid": original_uid })),
        ))
        .await;
    assert!(res.is_ok(), "{res:#?}");

    // Attempt to create another project with the same symbol should error
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_10",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "10",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project vitafast (0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_data_room_operations() {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create project (projects dataset is auto-created)
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
    );

    // This complex operation:
    // - Creates versioned file dataset
    // - Uploads new file version
    // - Links new file into the project data room
    //
    // In this request we are fetching back only "basic info" that is denormalized
    // to the data room entry.
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($ipnftUid: String!, $path: CollectionPath!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          uploadFile(
                            path: $path
                            content: $content
                            contentType: $contentType
                            changeBy: $changeBy
                            accessLevel: $accessLevel
                            description: $description
                            categories: $categories
                            tags: $tags
                            contentText: $contentText
                            encryptionMetadata: $encryptionMetadata
                          ) {
                            isSuccess
                            message
                            ... on MoleculeDataRoomFinishUploadFileResultSuccess {
                              entry {
                                project {
                                  account {
                                    accountName
                                  }
                                }
                                path
                                ref
                                asVersionedFile {
                                  latest {
                                    version
                                    contentHash
                                    contentType
                                    changeBy
                                    accessLevel
                                    description
                                    categories
                                    tags
                                    encryptionMetadata {
                                      dataToEncryptHash
                                      accessControlConditions
                                      encryptedBy
                                      encryptedAt
                                      chain
                                      litSdkVersion
                                      litNetwork
                                      templateName
                                      contractVersion
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "accessLevel": "public",
                "description": "Plain text file",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
                "contentText": "hello",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1",
                    "accessControlConditions": "EM2",
                    "encryptedBy": "EM3",
                    "encryptedAt": "EM4",
                    "chain": "EM5",
                    "litSdkVersion": "EM6",
                    "litNetwork": "EM7",
                    "templateName": "EM8",
                    "contractVersion": "EM9",
                },
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_1_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"],
        json!({
            "isSuccess": true,
            "message": "",
            "entry": {
                "project": {
                    "account": {
                        "accountName": "molecule.vitafast",
                    }
                },
                "path": "/foo.txt",
                "ref": file_1_did,
                "asVersionedFile": {
                    "latest": {
                        "version": 1,
                        "contentHash": "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392",
                        "contentType": "text/plain",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                        "accessLevel": "public",
                        "description": "Plain text file",
                        "encryptionMetadata": {
                            "accessControlConditions": "EM2",
                            "chain": "EM5",
                            "contractVersion": "EM9",
                            "dataToEncryptHash": "EM1",
                            "encryptedAt": "EM4",
                            "encryptedBy": "EM3",
                            "litNetwork": "EM7",
                            "litSdkVersion": "EM6",
                            "templateName": "EM8",
                        },
                        "categories": ["test-category"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                }
            }
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    // Very similar request to create another file, but this time we get back the
    // detailed properties that are not denormalized
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($ipnftUid: String!, $path: CollectionPath!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          uploadFile(
                            path: $path
                            content: $content
                            contentType: $contentType
                            changeBy: $changeBy
                            accessLevel: $accessLevel
                            description: $description
                            categories: $categories
                            tags: $tags
                            contentText: $contentText
                            encryptionMetadata: $encryptionMetadata
                          ) {
                            isSuccess
                            message
                            ... on MoleculeDataRoomFinishUploadFileResultSuccess {
                              entry {
                                path
                                ref
                                asVersionedFile {
                                  latest {
                                    version
                                    contentHash
                                    contentType
                                    changeBy
                                    accessLevel
                                    description
                                    categories
                                    tags
                                    contentText
                                    encryptionMetadata {
                                      dataToEncryptHash
                                      accessControlConditions
                                      encryptedBy
                                      encryptedAt
                                      chain
                                      litSdkVersion
                                      litNetwork
                                      templateName
                                      contractVersion
                                    }
                                    content
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/bar.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "accessLevel": "public",
                "description": "Plain text file",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
                "contentText": "hello",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1",
                    "accessControlConditions": "EM2",
                    "encryptedBy": "EM3",
                    "encryptedAt": "EM4",
                    "chain": "EM5",
                    "litSdkVersion": "EM6",
                    "litNetwork": "EM7",
                    "templateName": "EM8",
                    "contractVersion": "EM9",
                },
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_2_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"],
        json!({
            "isSuccess": true,
            "message": "",
            "entry": {
                "path": "/bar.txt",
                "ref": file_2_did,
                "asVersionedFile": {
                    "latest": {
                        "version": 1,
                        "contentHash": "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392",
                        "contentType": "text/plain",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                        "accessLevel": "public",
                        "description": "Plain text file",
                        "categories": ["test-category"],
                        "tags": ["test-tag1", "test-tag2"],
                        "contentText": "hello",
                        "encryptionMetadata": {
                            "dataToEncryptHash": "EM1",
                            "accessControlConditions": "EM2",
                            "encryptedBy": "EM3",
                            "encryptedAt": "EM4",
                            "chain": "EM5",
                            "litSdkVersion": "EM6",
                            "litNetwork": "EM7",
                            "templateName": "EM8",
                            "contractVersion": "EM9",
                        },
                        "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                    }
                }
            }
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    // Very similar request to create another file, but ensuring that optional
    // fields are indeed optional
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($ipnftUid: String!, $path: CollectionPath!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          uploadFile(
                            path: $path
                            content: $content
                            contentType: $contentType
                            changeBy: $changeBy
                            accessLevel: $accessLevel
                            description: $description
                            categories: $categories
                            tags: $tags
                            contentText: $contentText
                            encryptionMetadata: $encryptionMetadata
                          ) {
                            isSuccess
                            message
                            ... on MoleculeDataRoomFinishUploadFileResultSuccess {
                              entry {
                                path
                                ref
                                asVersionedFile {
                                  latest {
                                    version
                                    contentHash
                                    contentType
                                    changeBy
                                    accessLevel
                                    description
                                    categories
                                    tags
                                    contentText
                                    encryptionMetadata {
                                      dataToEncryptHash
                                      accessControlConditions
                                      encryptedBy
                                      encryptedAt
                                      chain
                                      litSdkVersion
                                      litNetwork
                                      templateName
                                      contractVersion
                                    }
                                    content
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/baz.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "accessLevel": "public",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_3_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"],
        json!({
            "isSuccess": true,
            "message": "",
            "entry": {
                "path": "/baz.txt",
                "ref": file_3_did,
                "asVersionedFile": {
                    "latest": {
                        "version": 1,
                        "contentHash": "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392",
                        "contentType": "text/plain",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                        "accessLevel": "public",
                        "description": null,
                        "categories": [],
                        "tags": [],
                        "contentText": null,
                        "encryptionMetadata": null,
                        "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                    }
                }
            }
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    // List data room entries and denormalized file fields
    const LIST_ENTRIES_QUERY: &str = indoc!(
        r#"
        query ($ipnftUid: String!) {
            molecule {
                v2 {
                    project(ipnftUid: $ipnftUid) {
                        dataRoom {
                            latest {
                                entries {
                                    totalCount
                                    nodes {
                                        path
                                        ref
                                        changeBy

                                        asVersionedFile {
                                            latest {
                                                contentType
                                                categories
                                                tags
                                                accessLevel
                                                version
                                                description
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_ENTRIES_QUERY).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 3,
            "nodes": [
                {
                    "path": "/bar.txt",
                    "ref": file_2_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": "Plain text file",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                {
                    "path": "/baz.txt",
                    "ref": file_3_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": null,
                            "categories": [],
                            "tags": [],
                        }
                    }
                },
                {
                    "path": "/foo.txt",
                    "ref": file_1_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": "Plain text file",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
            ],
        })
    );

    // Upload new version of an existing file
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($ipnftUid: String!, $ref: DatasetID!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          uploadFile(
                            ref: $ref
                            content: $content
                            contentType: $contentType
                            changeBy: $changeBy
                            accessLevel: $accessLevel
                            description: $description
                            categories: $categories
                            tags: $tags
                            contentText: $contentText
                            encryptionMetadata: $encryptionMetadata
                          ) {
                            isSuccess
                            message
                            ... on MoleculeDataRoomFinishUploadFileResultSuccess {
                              entry {
                                path
                                ref
                                asVersionedFile {
                                  latest {
                                    version
                                    contentHash
                                    contentType
                                    changeBy
                                    accessLevel
                                    description
                                    categories
                                    tags
                                    contentText
                                    encryptionMetadata {
                                      dataToEncryptHash
                                      accessControlConditions
                                      encryptedBy
                                      encryptedAt
                                      chain
                                      litSdkVersion
                                      litNetwork
                                      templateName
                                      contractVersion
                                    }
                                    content
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": file_1_did,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                "accessLevel": "public",
                "description": "Plain text file that was updated",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
                "contentText": "bye",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1",
                    "accessControlConditions": "EM2",
                    "encryptedBy": "EM3",
                    "encryptedAt": "EM4",
                    "chain": "EM5",
                    "litSdkVersion": "EM6",
                    "litNetwork": "EM7",
                    "templateName": "EM8",
                    "contractVersion": "EM9",
                },
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );

    assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"],
        json!({
            "isSuccess": true,
            "message": "",
            "entry": {
                "path": "/foo.txt",
                "ref": file_1_did,
                "asVersionedFile": {
                    "latest": {
                        "version": 2,
                        "contentHash": "f162040d234965143cf2113060344aec5c3ad74b34a5f713b16df21c6fc9349fb047b",
                        "contentType": "text/plain",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                        "accessLevel": "public",
                        "description": "Plain text file that was updated",
                        "categories": ["test-category"],
                        "tags": ["test-tag1", "test-tag2"],
                        "contentText": "bye",
                        "encryptionMetadata": {
                            "dataToEncryptHash": "EM1",
                            "accessControlConditions": "EM2",
                            "encryptedBy": "EM3",
                            "encryptedAt": "EM4",
                            "chain": "EM5",
                            "litSdkVersion": "EM6",
                            "litNetwork": "EM7",
                            "templateName": "EM8",
                            "contractVersion": "EM9",
                        },
                        "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye"),
                    }
                }
            }
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    // Get specific entry
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($ipnftUid: String!) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          latest {
                            entry(path: "/foo.txt") {
                              path
                              ref
                              changeBy
                              asVersionedFile {
                                latest {
                                  version
                                  contentHash
                                  contentType
                                  changeBy
                                  accessLevel
                                  description
                                  categories
                                  tags
                                  contentText
                                  encryptionMetadata {
                                    dataToEncryptHash
                                    accessControlConditions
                                    encryptedBy
                                    encryptedAt
                                    chain
                                    litSdkVersion
                                    litNetwork
                                    templateName
                                    contractVersion
                                  }
                                  content
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entry"],
        json!({
            "path": "/foo.txt",
            "ref": file_1_did,
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
            "asVersionedFile": {
                "latest": {
                    "version": 2,
                    "accessLevel": "public",
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    "contentHash": "f162040d234965143cf2113060344aec5c3ad74b34a5f713b16df21c6fc9349fb047b",
                    "contentText": "bye",
                    "contentType": "text/plain",
                    "description": "Plain text file that was updated",
                    "categories": ["test-category"],
                    "tags": ["test-tag1", "test-tag2"],
                    "encryptionMetadata": {
                        "dataToEncryptHash": "EM1",
                        "accessControlConditions": "EM2",
                        "encryptedBy": "EM3",
                        "encryptedAt": "EM4",
                        "chain": "EM5",
                        "litSdkVersion": "EM6",
                        "litNetwork": "EM7",
                        "templateName": "EM8",
                        "contractVersion": "EM9",
                    },
                    "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye"),
                }
            },
        }),
    );

    // Get specific entry with optional fields not set
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($ipnftUid: String!) {
                  molecule {
                    v2 {
                      project(ipnftUid: $ipnftUid) {
                        dataRoom {
                          latest {
                            entry(path: "/baz.txt") {
                              path
                              ref
                              changeBy
                              asVersionedFile {
                                latest {
                                  version
                                  contentHash
                                  contentType
                                  changeBy
                                  accessLevel
                                  description
                                  categories
                                  tags
                                  contentText
                                  encryptionMetadata {
                                    dataToEncryptHash
                                    accessControlConditions
                                    encryptedBy
                                    encryptedAt
                                    chain
                                    litSdkVersion
                                    litNetwork
                                    templateName
                                    contractVersion
                                  }
                                  content
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entry"],
        json!({
            "path": "/baz.txt",
            "ref": file_3_did,
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            "asVersionedFile": {
                "latest": {
                    "version": 1,
                    "accessLevel": "public",
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "contentHash": "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392",
                    "contentType": "text/plain",
                    "contentText": null,
                    "description": null,
                    "categories": [],
                    "tags": [],
                    "encryptionMetadata": null,
                    "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                }
            },
        }),
    );

    ///////////////
    // moveEntry //
    ///////////////
    const MOVE_ENTRY_QUERY: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $fromPath: CollectionPath!, $toPath: CollectionPath!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  moveEntry(fromPath: $fromPath, toPath: $toPath) {
                    isSuccess
                    message
                  }
                }
              }
            }
          }
        }
        "#
    );

    // Non-existent file
    assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "fromPath": "/non-existent-path.txt",
                "toPath": "/2025/foo.txt",
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "moveEntry": {
                                "isSuccess": false,
                                "message": "Data room entry not found",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual file move
    assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "fromPath": "/foo.txt",
                "toPath": "/2025/foo.txt",
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "moveEntry": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 3,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                {
                    "path": "/bar.txt",
                    "ref": file_2_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": "Plain text file",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                {
                    "path": "/baz.txt",
                    "ref": file_3_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": null,
                            "categories": [],
                            "tags": [],
                        }
                    }
                },
            ],
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    /////////////////
    // removeEntry //
    /////////////////
    const REMOVE_ENTRY_QUERY: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $path: CollectionPath!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  removeEntry(path: $path) {
                    isSuccess
                    message
                  }
                }
              }
            }
          }
        }
        "#
    );

    // Non-existent file
    assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/non-existent-path.txt",
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "removeEntry": {
                                "isSuccess": false,
                                "message": "Data room entry not found",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual file removal
    assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/baz.txt",
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "removeEntry": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 2,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                {
                    "path": "/bar.txt",
                    "ref": file_2_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": "Plain text file",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
            ],
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileRemovedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    ////////////////////////
    // updateFileMetadata //
    ////////////////////////
    const UPDATE_METADATA_QUERY: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $ref: DatasetID!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  updateFileMetadata(
                    ref: $ref
                    accessLevel: $accessLevel
                    changeBy: $changeBy
                    description: $description
                    categories: $categories
                    tags: $tags
                    contentText: $contentText
                    encryptionMetadata: $encryptionMetadata
                  ) {
                    isSuccess
                    message
                  }
                }
              }
            }
          }
        }
        "#
    );

    // Non-existent file
    let random_dataset_id = odf::DatasetID::new_generated_ed25519().1;

    assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_METADATA_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": random_dataset_id.to_string(),
                "accessLevel": "holder",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                "description": "Plain text file that was updated... again",
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag1", "test-tag2", "test-tag3"],
                "contentText": "bye bye bye",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1-updated",
                    "accessControlConditions": "EM2-updated",
                    "encryptedBy": "EM3-updated",
                    "encryptedAt": "EM4-updated",
                    "chain": "EM5-updated",
                    "litSdkVersion": "EM6-updated",
                    "litNetwork": "EM7-updated",
                    "templateName": "EM8-updated",
                    "contractVersion": "EM9-updated",
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "updateFileMetadata": {
                                "isSuccess": false,
                                "message": "Data room entry not found",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual update
    assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_METADATA_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": file_1_did,
                "accessLevel": "holder",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                "description": "Plain text file that was updated... again",
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag1", "test-tag2", "test-tag3"],
                "contentText": "bye bye bye",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1-updated",
                    "accessControlConditions": "EM2-updated",
                    "encryptedBy": "EM3-updated",
                    "encryptedAt": "EM4-updated",
                    "chain": "EM5-updated",
                    "litSdkVersion": "EM6-updated",
                    "litNetwork": "EM7-updated",
                    "templateName": "EM8-updated",
                    "contractVersion": "EM9-updated",
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "dataRoom": {
                            "updateFileMetadata": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 2,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "holder",
                            "contentType": "text/plain",
                            "version": 3,
                            "description": "Plain text file that was updated... again",
                            "categories": ["test-category-1", "test-category-2"],
                            "tags": ["test-tag1", "test-tag2", "test-tag3"],
                        }
                    }
                },
                {
                    "path": "/bar.txt",
                    "ref": file_2_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 1,
                            "description": "Plain text file",
                            "categories": ["test-category"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
            ],
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query ($ref: DatasetID!) {
                  datasets {
                    byId(datasetId: $ref) {
                      asVersionedFile {
                        latest {
                          extraData
                        }
                      }
                    }
                  }
                }
                "#
            ),
            async_graphql::Variables::from_json(json!({
                "ref": file_1_did,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["datasets"]["byId"]["asVersionedFile"]["latest"]["extraData"],
        json!({
            "categories": ["test-category-1", "test-category-2"],
            "content_text": "bye bye bye",
            "description": "Plain text file that was updated... again",
            "encryption_metadata": "{\"version\":0,\"dataToEncryptHash\":\"EM1-updated\",\"accessControlConditions\":\"EM2-updated\",\"encryptedBy\":\"EM3-updated\",\"encryptedAt\":\"EM4-updated\",\"chain\":\"EM5-updated\",\"litSdkVersion\":\"EM6-updated\",\"litNetwork\":\"EM7-updated\",\"templateName\":\"EM8-updated\",\"contractVersion\":\"EM9-updated\"}",
            "molecule_access_level": "holder",
            "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
            "tags": ["test-tag1", "test-tag2", "test-tag3"],
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileRemovedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    // !!!!!!!!!!!!!!!!!!!!!!!!!!! TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!
    //
    // Ensure our logic for converting entry path into file dataset name is
    // similar to what Molecule does on their side currently.
    //
    // Introduce tests for:
    // - Attempt to create new file with path that already exists - expect error
    // - Filter by: accessLevel
    // - Get entries with prefix and maxDepth
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_announcements_operations() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create the first project
    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    assert_eq!(
        {
            let res_json = GraphQLQueryRequest::new(
                CREATE_PROJECT,
                async_graphql::Variables::from_value(value!({
                    "ipnftSymbol": "vitafast",
                    "ipnftUid": PROJECT_1_UID,
                    "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                    "ipnftTokenId": "9",
                })),
            )
            .execute(&harness.schema, &harness.catalog_authorized)
            .await
            .data
            .into_json()
            .unwrap();

            res_json["molecule"]["v2"]["createProject"]["isSuccess"].as_bool()
        },
        Some(true),
    );

    // Announcements are empty
    const LIST_ANNOUNCEMENTS: &str = indoc!(
        r#"
        query ($ipnftUid: String!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                announcements {
                  tail {
                    totalCount
                    nodes {
                      id
                      headline
                      body
                      attachments
                      accessLevel
                      changeBy
                      categories
                      tags
                    }
                  }
                }
              }
            }
          }
        }
        "#
    );

    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "announcements": {
                            "tail": {
                                "totalCount": 0,
                                "nodes": []
                            }
                        }
                    }
                }
            }
        })
    );

    // Create a few versioned files to use as attachments
    const CREATE_VERSIONED_FILE: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $path: CollectionPath!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  uploadFile(
                    path: $path
                    content: $content
                    contentType: $contentType
                    changeBy: $changeBy
                    accessLevel: $accessLevel
                    description: $description
                    categories: $categories
                    tags: $tags
                    contentText: $contentText
                    encryptionMetadata: $encryptionMetadata
                  ) {
                    isSuccess
                    message
                    ... on MoleculeDataRoomFinishUploadFileResultSuccess {
                      entry {
                        ref
                      }
                    }
                  }
                }
              }
            }
          }
        }
        "#
    );

    let project_1_file_1_dataset_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello foo"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "accessLevel": "public",
                "description": "Plain text file (foo)",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
                "contentText": "hello foo",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1",
                    "accessControlConditions": "EM2",
                    "encryptedBy": "EM3",
                    "encryptedAt": "EM4",
                    "chain": "EM5",
                    "litSdkVersion": "EM6",
                    "litNetwork": "EM7",
                    "templateName": "EM8",
                    "contractVersion": "EM9",
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut upload_file_node =
            res_json["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"].take();
        // Extract node for simpler comparison
        let file_dataset_id = upload_file_node["entry"]["ref"]
            .take()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(
            upload_file_node,
            json!({
                "entry": {
                    "ref": null, // Extracted above
                },
                "isSuccess": true,
                "message": "",
            })
        );

        file_dataset_id
    };

    let project_1_file_2_dataset_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/bar.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello bar"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                "accessLevel": "public",
                "description": "Plain text file (bar)",
                "categories": [],
                "tags": [],
                "contentText": "hello bar",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1",
                    "accessControlConditions": "EM2",
                    "encryptedBy": "EM3",
                    "encryptedAt": "EM4",
                    "chain": "EM5",
                    "litSdkVersion": "EM6",
                    "litNetwork": "EM7",
                    "templateName": "EM8",
                    "contractVersion": "EM9",
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut upload_file_node =
            res_json["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"].take();
        // Extract node for simpler comparison
        let file_dataset_id = upload_file_node["entry"]["ref"]
            .take()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(
            upload_file_node,
            json!({
                "entry": {
                    "ref": null, // Extracted above
                },
                "isSuccess": true,
                "message": "",
            })
        );

        file_dataset_id
    };

    // Create an announcement without attachments
    const CREATE_ANNOUNCEMENT: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $headline: String!, $body: String!, $attachments: [DatasetID!], $moleculeAccessLevel: String!, $moleculeChangeBy: String!, $categories: [String!]!, $tags: [String!]!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                announcements {
                  create(
                    headline: $headline
                    body: $body
                    attachments: $attachments
                    moleculeAccessLevel: $moleculeAccessLevel
                    moleculeChangeBy: $moleculeChangeBy
                    categories: $categories
                    tags: $tags
                  ) {
                    isSuccess
                    message
                    __typename
                    ... on CreateAnnouncementSuccess {
                      announcementId
                    }
                  }
                }
              }
            }
          }
        }
        "#
    );

    let project_1_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag1", "test-tag2", "test-tag3"],
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut announcement_create_node =
            res_json["molecule"]["v2"]["project"]["announcements"]["create"].take();
        // Extract node for simpler comparison
        let new_announcement_id = announcement_create_node["announcementId"]
            .take()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(
            announcement_create_node,
            json!({
                "__typename": "CreateAnnouncementSuccess",
                "announcementId": null, // Extracted above
                "isSuccess": true,
                "message": "",
            })
        );

        new_announcement_id
    };

    // Create an announcement with one attachment
    let project_1_announcement_2_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 2",
                "body": "Blah blah 2",
                "attachments": [project_1_file_1_dataset_id],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                "categories": ["test-category-1"],
                "tags": ["test-tag1", "test-tag2"],
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut announcement_create_node =
            res_json["molecule"]["v2"]["project"]["announcements"]["create"].take();
        // Extract node for simpler comparison
        let new_announcement_id = announcement_create_node["announcementId"]
            .take()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(
            announcement_create_node,
            json!({
                "__typename": "CreateAnnouncementSuccess",
                "announcementId": null, // Extracted above
                "isSuccess": true,
                "message": "",
            })
        );

        new_announcement_id
    };

    // Create an announcement with two attachments
    let project_1_announcement_3_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 3",
                "body": "Blah blah 3",
                "attachments": [
                    project_1_file_1_dataset_id,
                    project_1_file_2_dataset_id,
                ],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                "categories": [],
                "tags": ["test-tag1"],
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut announcement_create_node =
            res_json["molecule"]["v2"]["project"]["announcements"]["create"].take();
        // Extract node for simpler comparison
        let new_announcement_id = announcement_create_node["announcementId"]
            .take()
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(
            announcement_create_node,
            json!({
                "__typename": "CreateAnnouncementSuccess",
                "announcementId": null, // Extracted above
                "isSuccess": true,
                "message": "",
            })
        );

        new_announcement_id
    };

    // Create an announcement with attachment DID that does not exist
    assert_eq!(
        GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 4",
                "body": "Blah blah 4",
                "attachments": [
                    project_1_file_1_dataset_id,
                    project_1_file_2_dataset_id,
                    odf::DatasetID::new_seeded_ed25519(b"does-not-exist").to_string(),
                ],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "categories": [],
                "tags": ["test-tag1"],
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "announcements": {
                            "create": {
                                "isSuccess": false,
                                "message": "Not found attachment(s): [did:odf:fed011ba79f25e520298ba6945dd6197083a366364bef178d5899b100c434748d88e5]",
                                "__typename": "CreateAnnouncementErrorInvalidAttachment",
                            }
                        }
                    }
                }
            }
        })
    );

    // Announcements are listed as expected
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "announcements": {
                            "tail": {
                                "totalCount": 3,
                                "nodes": [
                                    {
                                        "id": project_1_announcement_3_id,
                                        "headline": "Test announcement 3",
                                        "body": "Blah blah 3",
                                        "attachments": [
                                            project_1_file_1_dataset_id,
                                            project_1_file_2_dataset_id,
                                        ],
                                        "accessLevel": "holders",
                                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                        "categories": [],
                                        "tags": ["test-tag1"],
                                    },
                                    {
                                        "id": project_1_announcement_2_id,
                                        "headline": "Test announcement 2",
                                        "body": "Blah blah 2",
                                        "attachments": [
                                            project_1_file_1_dataset_id,
                                        ],
                                        "accessLevel": "holders",
                                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    },
                                    {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [],
                                        "accessLevel": "holders",
                                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                                        "categories": ["test-category-1", "test-category-2"],
                                        "tags": ["test-tag1", "test-tag2", "test-tag3"],
                                    },
                                ]
                            }
                        }
                    }
                }
            }
        })
    );

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_3_id,
                        "headline": "Test announcement 3",
                        "body": "Blah blah 3",
                        "attachments": [
                            project_1_file_1_dataset_id,
                            project_1_file_2_dataset_id,
                        ],
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                        "categories": [],
                        "tags": ["test-tag1"],
                    }
                },
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_2_id,
                        "headline": "Test announcement 2",
                        "body": "Blah blah 2",
                        "attachments": [
                            project_1_file_1_dataset_id,
                        ],
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                        "categories": ["test-category-1"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                },
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_1_id,
                        "headline": "Test announcement 1",
                        "body": "Blah blah 1",
                        "attachments": [],
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                        "categories": ["test-category-1", "test-category-2"],
                        "tags": ["test-tag1", "test-tag2", "test-tag3"],
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v2_activity() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    assert_eq!(
        {
            let res_json = GraphQLQueryRequest::new(
                CREATE_PROJECT,
                async_graphql::Variables::from_value(value!({
                    "ipnftSymbol": "vitafast",
                    "ipnftUid": PROJECT_1_UID,
                    "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                    "ipnftTokenId": "9",
                })),
            )
            .execute(&harness.schema, &harness.catalog_authorized)
            .await
            .data
            .into_json()
            .unwrap();

            res_json["molecule"]["v2"]["createProject"]["isSuccess"].as_bool()
        },
        Some(true),
    );

    // Activities are empty
    let expected_activity_node = value!({
        "activity": {
            "nodes": []
        }
    });
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": expected_activity_node
                }
            }
        })
    );

    /*
    // Create a few versioned files
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(CREATE_VERSIONED_FILE).variables(
                async_graphql::Variables::from_json(json!({
                    // TODO: Need ability  to create datasets with target AccountID
                    "datasetAlias": format!("{project_account_name}/test-file-1"),
                })),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let test_file_1 = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]
        ["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(CREATE_VERSIONED_FILE).variables(
                async_graphql::Variables::from_json(json!({
                    // TODO: Need ability  to create datasets with target AccountID
                    "datasetAlias": format!("{project_account_name}/test-file-2"),
                })),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let test_file_2 = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]
        ["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Upload new file versions
    const UPLOAD_NEW_VERSION: &str = indoc!(
        r#"
        mutation ($datasetId: DatasetID!, $content: Base64Usnp!) {
            datasets {
                byId(datasetId: $datasetId) {
                    asVersionedFile {
                        uploadNewVersion(content: $content) {
                            isSuccess
                            message
                        }
                    }
                }
            }
        }
        "#
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(UPLOAD_NEW_VERSION).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": &test_file_1,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"file 1"),
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["uploadNewVersion"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(UPLOAD_NEW_VERSION).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": &test_file_2,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"file 2"),
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["uploadNewVersion"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Link new file into the project data room
    const COLLECTION_ADD_ENTRY: &str = indoc!(
        r#"
        mutation ($datasetId: DatasetID!, $entry: CollectionEntryInput!) {
            datasets {
                byId(datasetId: $datasetId) {
                    asCollection {
                        addEntry(entry: $entry) {
                            isSuccess
                            message
                        }
                    }
                }
            }
        }
        "#
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(COLLECTION_ADD_ENTRY).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": data_room_did,
                "entry": {
                    "path": "/foo",
                    "ref": test_file_1,
                    "extraData": {
                        "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC"
                    },
                },
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["addEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(COLLECTION_ADD_ENTRY).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": data_room_did,
                "entry": {
                    "path": "/bar",
                    "ref": test_file_2,
                    "extraData": {
                        "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC"
                    },
                },
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["addEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Move a file (retract + append)
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
            mutation ($datasetId: DatasetID!, $pathFrom: CollectionPath!, $pathTo: CollectionPath!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        asCollection {
                            moveEntry(pathFrom: $pathFrom, pathTo: $pathTo) {
                                isSuccess
                                message
                            }
                        }
                    }
                }
            }
            "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &data_room_did,
                "pathFrom": "/bar",
                "pathTo": "/baz"
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["moveEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Update a file (correction from-to)
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
            mutation ($datasetId: DatasetID!, $pathFrom: CollectionPath!, $pathTo: CollectionPath!, $extraData: JSON) {
                datasets {
                    byId(datasetId: $datasetId) {
                        asCollection {
                            moveEntry(pathFrom: $pathFrom, pathTo: $pathTo, extraData: $extraData) {
                                isSuccess
                                message
                            }
                        }
                    }
                }
            }
            "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &data_room_did,
                "pathFrom": "/foo",
                "pathTo": "/foo",
                "extraData": {
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD"
                },
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["moveEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Create an announcement
    const CREATE_ANNOUNCEMENT: &str = indoc!(
        r#"
        mutation (
            $ipnftUid: String!,
            $headline: String!,
            $body: String!,
            $attachments: [String!],
            $moleculeAccessLevel: String!,
            $moleculeChangeBy: String!,
        ) {
            molecule {
                project(ipnftUid: $ipnftUid) {
                    createAnnouncement(
                        headline: $headline,
                        body: $body,
                        attachments: $attachments,
                        moleculeAccessLevel: $moleculeAccessLevel,
                        moleculeChangeBy: $moleculeChangeBy,
                    ) {
                        isSuccess
                        message
                    }
                }
            }
        }
        "#
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "headline": "Test announcement 1",
                "body": "Blah blah",
                "attachments": [test_file_1, test_file_2],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"]["createAnnouncement"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Upload new file version
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(UPLOAD_NEW_VERSION).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": &test_file_1,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"file 1 - updated"),
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["uploadNewVersion"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Remove a file
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
            mutation ($datasetId: DatasetID!, $path: CollectionPath!) {
                datasets {
                    byId(datasetId: $datasetId) {
                        asCollection {
                            removeEntry(path: $path) {
                                isSuccess
                                message
                            }
                        }
                    }
                }
            }
            "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &data_room_did,
                "path": "/bar",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["removeEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Check project activity events
    const LIST_EVENTS: &str = indoc!(
        r#"
        query ($ipnftUid: String!) {
            molecule {
                project(ipnftUid: $ipnftUid) {
                    activity {
                        nodes {
                            __typename
                            ... on MoleculeProjectEventDataRoomEntryAdded {
                                entry {
                                    path
                                }
                            }
                            ... on MoleculeProjectEventDataRoomEntryRemoved {
                                entry {
                                    path
                                }
                            }
                            ... on MoleculeProjectEventDataRoomEntryUpdated {
                                newEntry {
                                    path
                                }
                            }
                            ... on MoleculeProjectEventAnnouncement {
                                announcement
                            }
                            ... on MoleculeProjectEventFileUpdated {
                                dataset { alias }
                                newEntry {
                                    version
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_EVENTS).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let mut json = res.data.into_json().unwrap();
    let nodes = &mut json["molecule"]["project"]["activity"]["nodes"];

    let any = serde_json::Value::Null;
    nodes[1]["announcement"]["announcement_id"] = any.clone();
    nodes[1]["announcement"]["system_time"] = any.clone();
    nodes[1]["announcement"]["event_time"] = any.clone();

    assert_eq!(
        *nodes,
        json!([
            {
                "__typename": "MoleculeProjectEventFileUpdated",
                "dataset": {
                    "alias": "molecule.vitafast/test-file-1",
                },
                "newEntry": {
                    "version": 2,
                },
            },
            {
                "__typename": "MoleculeProjectEventAnnouncement",
                "announcement": {
                    "announcement_id": &any,
                    "attachments": [&test_file_1, &test_file_2],
                    "body": "Blah blah",
                    "event_time": &any,
                    "headline": "Test announcement 1",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                },
            },
            {
                "__typename": "MoleculeProjectEventDataRoomEntryUpdated",
                "newEntry": {
                    "path": "/foo",
                },
            },
            {
                "__typename": "MoleculeProjectEventDataRoomEntryAdded",
                "entry": {
                    "path": "/baz",
                },
            },
            {
                "__typename": "MoleculeProjectEventDataRoomEntryRemoved",
                "entry": {
                    "path": "/bar",
                },
            },
            {
                "__typename": "MoleculeProjectEventDataRoomEntryAdded",
                "entry": {
                    "path": "/bar",
                },
            },
            {
                "__typename": "MoleculeProjectEventDataRoomEntryAdded",
                "entry": {
                    "path": "/foo",
                },
            },
            {
                "__typename": "MoleculeProjectEventFileUpdated",
                "dataset": {
                    "alias": "molecule.vitafast/test-file-2",
                },
                "newEntry": {
                    "version": 1,
                },
            },
            {
                "__typename": "MoleculeProjectEventFileUpdated",
                "dataset": {
                    "alias": "molecule.vitafast/test-file-1",
                },
                "newEntry": {
                    "version": 1,
                },
            },
        ])
    );

    ///////////////////////////////////////////////////////////////////////////////

    // Create another project
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitaslow",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2_10",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "ipnftTokenId": "10",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");

    // Create an announcement
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2_10",
                "headline": "Test announcement 2",
                "body": "Blah blah bleh",
                "attachments": [],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"]["createAnnouncement"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Check global activity events
    const LIST_ACTIVITY: &str = indoc!(
        r#"
        query {
            molecule {
                activity {
                    nodes {
                        __typename
                        project {
                            ipnftSymbol
                        }
                        ... on MoleculeProjectEventDataRoomEntryAdded {
                            entry {
                                path
                            }
                        }
                        ... on MoleculeProjectEventDataRoomEntryRemoved {
                            entry {
                                path
                            }
                        }
                        ... on MoleculeProjectEventDataRoomEntryUpdated {
                            newEntry {
                                path
                            }
                        }
                        ... on MoleculeProjectEventAnnouncement {
                            announcement
                        }
                        ... on MoleculeProjectEventFileUpdated {
                            dataset { alias }
                            newEntry {
                                version
                            }
                        }
                    }
                }
            }
        }
        "#
    );
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_ACTIVITY))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let mut json = res.data.into_json().unwrap();
    let nodes = &mut json["molecule"]["activity"]["nodes"];
    nodes[0]["announcement"]["announcement_id"] = any.clone();
    nodes[0]["announcement"]["system_time"] = any.clone();
    nodes[0]["announcement"]["event_time"] = any.clone();
    nodes[1]["announcement"]["announcement_id"] = any.clone();
    nodes[1]["announcement"]["system_time"] = any.clone();
    nodes[1]["announcement"]["event_time"] = any.clone();

    // NOTE: Only announcements are currently supported
    assert_eq!(
        *nodes,
        json!([
            {
                "__typename": "MoleculeProjectEventAnnouncement",
                "project": {
                    "ipnftSymbol": "vitaslow",
                },
                "announcement": {
                    "announcement_id": &any,
                    "attachments": [],
                    "body": "Blah blah bleh",
                    "event_time": &any,
                    "headline": "Test announcement 2",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                },
            },
            {
                "__typename": "MoleculeProjectEventAnnouncement",
                "project": {
                    "ipnftSymbol": "vitafast",
                },
                "announcement": {
                    "announcement_id": &any,
                    "attachments": [&test_file_1, &test_file_2],
                    "body": "Blah blah",
                    "event_time": &any,
                    "headline": "Test announcement 1",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                },
            },
        ])
    );
     */
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
