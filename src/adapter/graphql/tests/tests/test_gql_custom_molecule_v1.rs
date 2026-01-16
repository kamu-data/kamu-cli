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
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    DatasetRegistry,
    DatasetRegistryExt,
};
use messaging_outbox::OutboxProvider;
use odf::dataset::MetadataChainExt;
use serde_json::json;

use crate::utils::{
    AuthenticationCatalogsResult,
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs_ext,
};

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
async fn test_molecule_v1_provision_project() {
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
                projects(page: 0, perPage: 100) {
                    nodes {
                        ipnftSymbol
                        ipnftUid
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
                "ipnftSymbol": "VITAFAST",
                "ipnftUid": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9",
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "9",
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
                            ipnftSymbol
                            ipnftUid
                            ipnftAddress
                            ipnftTokenId
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
        res.data.into_json().unwrap()["molecule"]["project"],
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["projects"]["nodes"],
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"],
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["createProject"],
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
async fn test_molecule_v1_data_room_operations() {
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
                // V2 fields
                "categories": null,
                "content_hash": null,
                "content_length": null,
                "content_type": null,
                "description": null,
                "molecule_access_level": null,
                "tags": null,
                "version": null,
            },
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v1_announcements_operations() {
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
                "moleculeAccessLevel": "holders",
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
                "moleculeAccessLevel": "holders",
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
                "moleculeAccessLevel": "holders",
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
                "moleculeAccessLevel": "holders",
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
                "moleculeAccessLevel": "holders",
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
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [],
                    "body": "Blah blah",
                    "categories": [],
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 1",
                    "offset": 0,
                    "op": 0,
                    "system_time": any,
                    "tags": [],
                },
                {
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [test_file_1],
                    "body": "Blah blah",
                    "categories": [],
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 2",
                    "offset": 1,
                    "op": 0,
                    "system_time": any,
                    "tags": [],
                },
                {
                    "molecule_access_level": "holders",
                    "announcement_id": any,
                    "attachments": [test_file_1, test_file_2],
                    "body": "Blah blah",
                    "categories": [],
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "event_time": any,
                    "headline": "Test announcement 3",
                    "offset": 2,
                    "op": 0,
                    "system_time": any,
                    "tags": [],
                },
            ]
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_molecule_v1_activity() {
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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

    pretty_assertions::assert_eq!(
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
                    "categories": [],
                    "event_time": &any,
                    "headline": "Test announcement 1",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                    "tags": [],
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
                    "categories": [],
                    "event_time": &any,
                    "headline": "Test announcement 2",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                    "tags": [],
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
                    "categories": [],
                    "body": "Blah blah",
                    "event_time": &any,
                    "headline": "Test announcement 1",
                    "molecule_access_level": "holders",
                    "molecule_change_by": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "system_time": &any,
                    "tags": [],
                },
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
pub struct GraphQLMoleculeV1Harness {
    base_gql_harness: BaseGQLDatasetHarness,
    pub schema: kamu_adapter_graphql::Schema,
    pub catalog_authorized: dill::Catalog,
    pub molecule_account_id: odf::AccountID,
}

#[bon]
impl GraphQLMoleculeV1Harness {
    #[builder]
    pub async fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
        #[builder(default = PredefinedAccountOpts {
            is_admin: false,
            can_provision_accounts: true,
        })]
        predefined_account_opts: PredefinedAccountOpts,
    ) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer)
            .build();

        let base_gql_catalog = base_gql_harness.catalog();

        let cache_dir = base_gql_harness.temp_dir().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let mut base_builder = dill::CatalogBuilder::new_chained(base_gql_catalog);
        base_builder
            .add::<kamu_datasets_services::FindVersionedFileVersionUseCaseImpl>()
            .add::<kamu_datasets_services::UpdateVersionedFileUseCaseImpl>()
            .add::<kamu_datasets_services::ViewVersionedFileHistoryUseCaseImpl>()
            .add::<kamu_datasets_services::FindCollectionEntriesUseCaseImpl>()
            .add::<kamu_datasets_services::UpdateCollectionEntriesUseCaseImpl>()
            .add::<kamu_datasets_services::ViewCollectionEntriesUseCaseImpl>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedBatchQuery::default())
            .add::<kamu::QueryServiceImpl>()
            .add::<kamu::QueryDatasetDataUseCaseImpl>()
            .add::<kamu::SessionContextBuilder>()
            .add::<kamu::ObjectStoreRegistryImpl>()
            .add::<kamu::ObjectStoreBuilderLocalFs>()
            .add_value(kamu::EngineConfigDatafusionEmbeddedIngest::default())
            .add::<kamu::EngineProvisionerNull>()
            .add::<kamu::DataFormatRegistryImpl>()
            .add::<kamu::PushIngestPlannerImpl>()
            .add::<kamu::PushIngestExecutorImpl>()
            .add::<kamu::PushIngestDataUseCaseImpl>()
            .add::<kamu_adapter_http::platform::UploadServiceLocal>()
            .add_value(kamu_core::utils::paths::CacheDir::new(cache_dir))
            .add_value(kamu_core::ServerUrlConfig::new_test(None))
            .add_value(kamu::domain::FileUploadLimitConfig::new_in_bytes(100_500))
            .add_value(
                kamu_adapter_graphql::GqlFeatureFlags::new()
                    .with_feature(kamu_adapter_graphql::GqlFeature::MoleculeApiV1),
            );

        kamu_molecule_services::register_dependencies(&mut base_builder);

        let base_catalog = base_builder.build();

        let molecule_account_id = odf::AccountID::new_generated_ed25519().1;

        let AuthenticationCatalogsResult {
            catalog_authorized, ..
        } = authentication_catalogs_ext(
            &base_catalog,
            Some(CurrentAccountSubject::Logged(LoggedAccount {
                account_id: molecule_account_id.clone(),
                account_name: "molecule".parse().unwrap(),
            })),
            predefined_account_opts,
        )
        .await;

        Self {
            base_gql_harness,
            schema: kamu_adapter_graphql::schema_quiet(),
            catalog_authorized,
            molecule_account_id,
        }
    }

    pub async fn create_projects_dataset(&self) -> CreateDatasetResult {
        let snapshot =
            kamu_molecule_domain::MoleculeDatasetSnapshots::projects("molecule".parse().unwrap());

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
        let catalog = self.catalog_authorized.clone();
        self.schema
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(&catalog))
                    .data(dataset_handle_data_loader(&catalog))
                    .data(catalog),
            )
            .await
    }

    pub async fn projects_metadata_chain_len(&self, dataset_alias: &odf::DatasetAlias) -> usize {
        let dataset_reg = self
            .catalog_authorized
            .get_one::<dyn DatasetRegistry>()
            .unwrap();
        let projects_dataset = dataset_reg
            .get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        let last_block = projects_dataset
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await
            .unwrap();

        usize::try_from(last_block.sequence_number).unwrap() + 1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
