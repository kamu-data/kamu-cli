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
use kamu_core::*;
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
                mutation (
                    $ipnftUid: String!
                    $path: String!
                    $content: Base64Usnp!
                    $contentType: String!
                    $changeBy: String!
                    $accessLevel: String!
                    $description: String
                    $categories: [String!]
                    $tags: [String!]
                    $contentText: String
                    $encryptionMetadata: String
                ) {
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
                                                    account { accountName }
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
                "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                        "categories": ["test-category"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                }
            }
        })
    );

    // Global activity
    // TODO: find a way to output tags/categories
    const LIST_GLOBAL_ACTIVITY_QUERY: &str = indoc!(
        r#"
        {
          molecule {
            v2 {
              activity {
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
                }
              }
            }
          }
        }
        "#
    );
    const LIST_PROJECT_ACTIVITY_QUERY: &str = indoc!(
        r#"
        query ($ipnftUid: String!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                activity {
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
                  }
                }
              }
            }
          }
        }
        "#
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
            async_graphql::Variables::default(),
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
                mutation (
                    $ipnftUid: String!
                    $path: String!
                    $content: Base64Usnp!
                    $contentType: String!
                    $changeBy: String!
                    $accessLevel: String!
                    $description: String
                    $categories: [String!]
                    $tags: [String!]
                    $contentText: String
                    $encryptionMetadata: String
                ) {
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
                                                        encryptionMetadata
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
                "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                        "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                "ipnftUid": ipnft_uid,
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
                mutation (
                    $ipnftUid: String!
                    $path: String!
                    $content: Base64Usnp!
                    $contentType: String!
                    $changeBy: String!
                    $accessLevel: String!
                    $description: String
                    $categories: [String!]
                    $tags: [String!]
                    $contentText: String
                    $encryptionMetadata: String
                ) {
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
                                                        encryptionMetadata
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
                "ipnftUid": ipnft_uid,
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
                mutation (
                    $ipnftUid: String!
                    $ref: DatasetID!
                    $content: Base64Usnp!
                    $contentType: String!
                    $changeBy: String!
                    $accessLevel: String!
                    $description: String
                    $categories: [String!]
                    $tags: [String!]
                    $contentText: String
                    $encryptionMetadata: String
                ) {
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
                                                        encryptionMetadata
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
                "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                        "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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
    // TODO: unlock after moveEntry/removeEntry updates
    /*assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::default(),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );*/
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
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
                                                    encryptionMetadata
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
                    "encryptionMetadata": r#"{"encryption": "lit"}"#,
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
                                                    encryptionMetadata
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
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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
    // TODO: unlock after moveEntry/removeEntry updates
    /*assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::default(),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );*/
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
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
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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
    // TODO: unlock after moveEntry/removeEntry updates
    /*assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::default(),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );*/
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
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
        mutation ($ipnftUid: String!, $ref: DatasetID!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: String) {
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
                "encryptionMetadata": r#"{"encryption": "lit", "chain": 1 }"#,
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
                "encryptionMetadata": r#"{"encryption": "lit", "chain": 1 }"#,
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
            "encryption_metadata": "{\"encryption\": \"lit\", \"chain\": 1 }",
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
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
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
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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
    // TODO: unlock after moveEntry/removeEntry updates
    /*assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::default(),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": expected_activity_node.clone()
            }
        })
    );*/
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
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

/*
#[test_log::test(tokio::test)]
async fn test_molecule_v2_announcements_operations() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create project (projects dataset is auto-created)
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res = &res.data.into_json().unwrap()["molecule"]["createProject"];
    let project_account_name = res["project"]["account"]["accountName"].as_str().unwrap();
    let announcements_did = res["project"]["announcements"]["id"].as_str().unwrap();

    // Announcements are empty
    const LIST_ANNOUNCEMENTS: &str = indoc!(
        r#"
        query ($datasetId: DatasetID!) {
            datasets {
                byId(datasetId: $datasetId) {
                    data {
                        tail(dataFormat: JSON_AOS) {
                            ... on DataQueryResultSuccess {
                                data { content }
                            }
                        }
                    }
                }
            }
        }
        "#
    );

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_ANNOUNCEMENTS).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": announcements_did,
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let content = res.data.into_json().unwrap()["datasets"]["byId"]["data"]["tail"]["data"]
        ["content"]
        .as_str()
        .unwrap()
        .to_string();
    let content: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert_eq!(content, json!([]));

    // Create a few versioned files to use as attachments
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

    // Create an announcement without attachments
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

    // Create an announcement with one attachment
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "headline": "Test announcement 2",
                "body": "Blah blah",
                "attachments": [test_file_1],
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

    // Create an announcement with two attachments
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "headline": "Test announcement 3",
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

    // Create an announcement with invalid attachment DID
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "headline": "Test announcement 3",
                "body": "Blah blah",
                "attachments": ["x"],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"]["createAnnouncement"],
        json!({
            "isSuccess": false,
            "message": "Value 'x' is not a valid did:odf",
        })
    );

    // Create an announcement with attachment DID that does not exist
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "headline": "Test announcement 3",
                "body": "Blah blah",
                "attachments": [odf::DatasetID::new_seeded_ed25519(b"does-not-exist").to_string()],
                "moleculeAccessLevel": "holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"]["createAnnouncement"],
        json!({
            "isSuccess": false,
            "message": "Dataset did:odf:fed011ba79f25e520298ba6945dd6197083a366364bef178d5899b100c434748d88e5 not found",
        })
    );

    // Announcements are listed as expected
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_ANNOUNCEMENTS).variables(
            async_graphql::Variables::from_json(json!({
                "datasetId": announcements_did,
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let content = res.data.into_json().unwrap()["datasets"]["byId"]["data"]["tail"]["data"]
        ["content"]
        .as_str()
        .unwrap()
        .to_string();
    let mut content: serde_json::Value = serde_json::from_str(&content).unwrap();
    let any = "<any>";
    content.as_array_mut().unwrap().iter_mut().for_each(|r| {
        let obj = r.as_object_mut().unwrap();
        obj["system_time"] = any.to_string().into();
        obj["event_time"] = any.to_string().into();
        obj["announcement_id"] = any.to_string().into();
    });
    assert_eq!(
        content,
        json!(
            [
                {
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [],
                    "body": "Blah blah",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 1",
                    "offset": 0,
                    "op": 0,
                    "system_time": any,
                },
                {
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [test_file_1],
                    "body": "Blah blah",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 2",
                    "offset": 1,
                    "op": 0,
                    "system_time": any,
                },
                {
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [test_file_1, test_file_2],
                    "body": "Blah blah",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 3",
                    "offset": 2,
                    "op": 0,
                    "system_time": any,
                },
            ]
        )
    );
}
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
#[test_log::test(tokio::test)]
async fn test_molecule_v2_activity() {
    let harness = GraphQLMoleculeV1Harness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create project (projects dataset is auto-created)
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res = &res.data.into_json().unwrap()["molecule"]["createProject"];
    let project_account_name = res["project"]["account"]["accountName"].as_str().unwrap();
    let data_room_did = res["project"]["dataRoom"]["id"].as_str().unwrap();

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
}
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
