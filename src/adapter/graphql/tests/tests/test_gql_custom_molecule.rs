// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine as _;
use bon::bon;
use indoc::indoc;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_core::*;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetResult};
use kamu_datasets_services::*;
use serde_json::json;

use crate::utils::{authentication_catalogs_ext, BaseGQLDatasetHarness, PredefinedAccountOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CREATE_PROJECT: &str = indoc!(
    r#"
    mutation (
        $ipnftAddress: String!,
        $ipnftTokenId: Int!,
        $ipnftUid: String!,
        $iptAddress: String!,
        $iptSymbol: String!,
    ) {
        molecule {
            createProject(
                ipnftAddress: $ipnftAddress,
                ipnftTokenId: $ipnftTokenId,
                ipnftUid: $ipnftUid,
                iptAddress: $iptAddress,
                iptSymbol: $iptSymbol,
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
    "#
);

const CREATE_VERSIONED_FILE: &str = indoc!(
    r#"
    mutation ($datasetAlias: DatasetAlias!) {
        datasets {
            createVersionedFile(
                datasetAlias: $datasetAlias,
                datasetVisibility: PRIVATE,
            ) {
                ... on CreateDatasetResultSuccess {
                    dataset {
                        id
                    }
                }
            }
        }
    }
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_provision_project() {
    let harness = GraphQLDatasetsHarness::builder()
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
                projects(page: 0, perPage: 100) {
                    nodes {
                        ipnftUid
                        iptSymbol
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["projects"]["nodes"],
        json!([]),
    );

    // Create first project
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 9,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "iptSymbol": "vitafast",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res = &res.data.into_json().unwrap()["molecule"]["createProject"];
    let project_account_id = res["project"]["account"]["id"].as_str().unwrap();
    let project_account_name = res["project"]["account"]["accountName"].as_str().unwrap();
    let data_room_did = res["project"]["dataRoom"]["id"].as_str().unwrap();
    let announcements_did = res["project"]["announcements"]["id"].as_str().unwrap();
    assert_ne!(project_account_id, harness.molecule_account_id.to_string());
    assert_eq!(project_account_name, "molecule.vitafast");
    pretty_assertions::assert_eq!(
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
                        project(ipnftUid: $ipnftUid) {
                            account { id accountName }
                            ipnftAddress
                            ipnftTokenId
                            ipnftUid
                            iptAddress
                            iptSymbol
                        }
                    }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 9,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "iptSymbol": "vitafast",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"],
        json!({
            "account": {
                "id": project_account_id,
                "accountName": project_account_name,
            },
            "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
            "ipnftTokenId": 9,
            "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
            "iptSymbol": "vitafast",
        }),
    );

    // Project appears in the list
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["projects"]["nodes"],
        json!([{
            "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            "iptSymbol": "vitafast",
        }]),
    );

    // Ensure errors on ipnftUid collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 9,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc3",
                "iptSymbol": "vitaslow",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project 0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9 (vitafast)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );

    // Ensure errors on iptSymbol collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 1,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_1",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc3",
                "iptSymbol": "vitafast",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project 0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9 (vitafast)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );

    // Ensure errors on iptAddress collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 1,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_1",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "iptSymbol": "vitaslow",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project 0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9 (vitafast)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );

    // Create another project
    // Ensure errors on ipnftUid collision
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 10,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_10",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc3",
                "iptSymbol": "vitaslow",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"]["isSuccess"],
        json!(true),
    );

    // Both projects appear in the list
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["projects"]["nodes"],
        json!([
            {
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptSymbol": "vitafast",
            },
            {
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_10",
                "iptSymbol": "vitaslow",
            }
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_data_room_operations() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create project (projects dataset is auto-created)
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 9,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "iptSymbol": "vitafast",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res = &res.data.into_json().unwrap()["molecule"]["createProject"];
    let project_account_name = res["project"]["account"]["accountName"].as_str().unwrap();
    let data_room_did = res["project"]["dataRoom"]["id"].as_str().unwrap();

    // Create versioned file
    // Molecule account can create new dataset in project org
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(CREATE_VERSIONED_FILE).variables(
                async_graphql::Variables::from_json(json!({
                    // TODO: Need ability  to create datasets with target AccountID
                    "datasetAlias": format!("{project_account_name}/test-file"),
                })),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let test_file_did = res.data.into_json().unwrap()["datasets"]["createVersionedFile"]["dataset"]
        ["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Upload new file version
    // Molecule should have write access to datasets it creates in the project
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
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
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": &test_file_did,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asVersionedFile"]["uploadNewVersion"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Link new file into the project data room
    // Molecule should have write access to core datasets
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
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
            ))
            .variables(async_graphql::Variables::from_json(json!({
                "datasetId": data_room_did,
                "entry": {
                    "path": "/foo",
                    "ref": test_file_did,
                    "extraData": {
                        "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC"
                    },
                },
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["datasets"]["byId"]["asCollection"]["addEntry"],
        json!({
            "isSuccess": true,
            "message": "",
        })
    );

    // Go from project to a file in one query
    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
                query ($ipnftUid: String!) {
                    molecule {
                        project(ipnftUid: $ipnftUid) {
                            dataRoom {
                                asCollection {
                                    latest {
                                        entry(path: "/foo") {
                                            extraData
                                            asDataset {
                                                asVersionedFile {
                                                    latest {
                                                        content
                                                        extraData
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
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["project"]["dataRoom"]["asCollection"]["latest"]
            ["entry"],
        json!({
            "asDataset": {
                "asVersionedFile": {
                    "latest": {
                        "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
                        "extraData": {},
                    }
                }
            },
            "extraData": {
                "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            },
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_announcements_operations() {
    let harness = GraphQLDatasetsHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create project (projects dataset is auto-created)
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": 9,
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "iptAddress": "0xdaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                "iptSymbol": "vitafast",
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
    pretty_assertions::assert_eq!(content, json!([]));

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
                "moleculeAccessLevel": "ipt-holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
                "moleculeAccessLevel": "ipt-holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
                "moleculeAccessLevel": "ipt-holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
                "moleculeAccessLevel": "ipt-holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
                "moleculeAccessLevel": "ipt-holders",
                "moleculeChangeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
        content,
        json!(
            [
                {
                    "molecule_access_level": "ipt-holders",
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
                    "molecule_access_level": "ipt-holders",
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
                    "molecule_access_level": "ipt-holders",
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLDatasetsHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
    molecule_account_id: odf::AccountID,
}

#[bon]
impl GraphQLDatasetsHarness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .build();

        let cache_dir = base_gql_harness.temp_dir().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let base_catalog = dill::CatalogBuilder::new_chained(base_gql_harness.catalog())
            .add::<RenameDatasetUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedBatchQuery::default())
            .add::<kamu::QueryServiceImpl>()
            .add::<kamu::ObjectStoreRegistryImpl>()
            .add::<kamu::ObjectStoreBuilderLocalFs>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedIngest::default())
            .add::<kamu::EngineProvisionerNull>()
            .add::<kamu::DataFormatRegistryImpl>()
            .add::<kamu::PushIngestPlannerImpl>()
            .add::<kamu::PushIngestExecutorImpl>()
            .add::<kamu::PushIngestDataUseCaseImpl>()
            .build();

        let molecule_account_id = odf::AccountID::new_generated_ed25519().1;

        let (_catalog_anonymous, catalog_authorized) = authentication_catalogs_ext(
            &base_catalog,
            Some(CurrentAccountSubject::Logged(LoggedAccount {
                account_id: molecule_account_id.clone(),
                account_name: "molecule".parse().unwrap(),
            })),
            PredefinedAccountOpts {
                is_admin: false,
                can_provision_accounts: true,
            },
        )
        .await;

        Self {
            base_gql_harness,
            catalog_authorized,
            molecule_account_id,
        }
    }

    pub async fn create_projects_dataset(&self) -> CreateDatasetResult {
        let snapshot = kamu_adapter_graphql::molecule::Molecule::dataset_snapshot_projects(
            "molecule/projects".parse().unwrap(),
        );

        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset
            .execute(snapshot, Default::default())
            .await
            .unwrap()
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(query.into().data(self.catalog_authorized.clone()))
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
