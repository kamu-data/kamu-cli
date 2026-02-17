// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Request, Variables};
use base64::Engine as _;
use indoc::indoc;
use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use serde_json::{Value, json};
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MULTITENANT_MOLECULE_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      auth:
        allowAnonymous: false
        users:
          predefined:
            - accountName: kamu
              password: kamu.dev
              email: kamu@example.com
              properties: [ admin, canProvisionAccounts ]
            - accountName: molecule
              password: molecule.dev
              email: molecule@example.com
              properties: [ admin, canProvisionAccounts ]
      quotaDefaults:
        storage: 1000000000
    "#
);

pub const ELASTICSEARCH_CONFIG_EXTENSION: &str = indoc::indoc!(
    r#"
      search:
        repo:
          kind: elasticsearch
          # url: https://localhost:9200
          url: http://localhost:9200
          password: root
          indexPrefix: <random_prefix>

          # Bind absolute path for HTTPS with self-signed certs
          # caCertPemPath: .local/elasticsearch/certs/ca/ca.crt

        embeddingsEncoder:
          kind: dummy
    "#
);

pub fn get_multitenant_molecule_config_with_elasticsearch() -> String {
    let random_prefix = random_strings::get_random_name(Some("kamu-test-e2e-"), 10).to_lowercase();

    let elasticsearch_config =
        ELASTICSEARCH_CONFIG_EXTENSION.replace("<random_prefix>", &random_prefix);

    // Parse both YAML configs
    let mut base_config: serde_yaml::Value =
        serde_yaml::from_str(MULTITENANT_MOLECULE_CONFIG).unwrap();
    let search_config: serde_yaml::Value = serde_yaml::from_str(&elasticsearch_config).unwrap();

    // Merge search config into content section
    if let (Some(content_map), Some(search_map)) = (
        base_config
            .get_mut("content")
            .and_then(|c| c.as_mapping_mut()),
        search_config.as_mapping(),
    ) {
        for (key, value) in search_map {
            content_map.insert(key.clone(), value.clone());
        }
    }

    serde_yaml::to_string(&base_config).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_molecule_v2_data_room_quota_exceeded(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    const ACCOUNT: &str = "molecule";
    const PASSWORD: &str = "molecule.dev";

    kamu_api_server_client
        .auth()
        .login_with_password(ACCOUNT, PASSWORD)
        .await;

    const IPNFT_ADDRESS: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1";
    const IPNFT_TOKEN_ID: &str = "1201";
    let ipnft_uid = format!("{IPNFT_ADDRESS}_{IPNFT_TOKEN_ID}");

    let create_project_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(CREATE_PROJECT).variables(Variables::from_json(json!({
                "ipnftSymbol": "quota-room-e2e",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": IPNFT_ADDRESS,
                "ipnftTokenId": IPNFT_TOKEN_ID,
            }))),
        )
        .await;
    let create_project_json = create_project_res
        .data
        .clone()
        .into_json()
        .unwrap_or_default();
    assert_eq!(
        create_project_json["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
        "Project creation failed: {create_project_res:?}"
    );

    kamu_api_server_client
        .account()
        .set_account_quota_bytes(ACCOUNT, 1)
        .await;

    let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(b"hello");
    let upload_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(DATA_ROOM_UPLOAD).variables(Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/quota.txt",
                "content": encoded,
            }))),
        )
        .await;

    assert!(
        upload_res.errors.is_empty(),
        "Upload mutation failed: {upload_res:?}"
    );
    let payload = upload_res.data.into_json().unwrap_or_default()["molecule"]["v2"]["project"]
        ["dataRoom"]["uploadFile"]
        .clone();
    assert_eq!(payload["isSuccess"], json!(false), "{payload:?}");
    let msg = payload["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("Quota exceeded"),
        "unexpected message: {payload:?}"
    );
    assert_eq!(payload["__typename"], json!("MoleculeQuotaExceeded"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_molecule_v2_announcements_quota_exceeded(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    const ACCOUNT: &str = "molecule";
    const PASSWORD: &str = "molecule.dev";

    kamu_api_server_client
        .auth()
        .login_with_password(ACCOUNT, PASSWORD)
        .await;

    const IPNFT_ADDRESS: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1";
    const IPNFT_TOKEN_ID: &str = "1202";
    let ipnft_uid = format!("{IPNFT_ADDRESS}_{IPNFT_TOKEN_ID}");

    let create_project_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(CREATE_PROJECT).variables(Variables::from_json(json!({
                "ipnftSymbol": "quota-announce-e2e",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": IPNFT_ADDRESS,
                "ipnftTokenId": IPNFT_TOKEN_ID,
            }))),
        )
        .await;
    let create_project_json = create_project_res
        .data
        .clone()
        .into_json()
        .unwrap_or_default();
    assert_eq!(
        create_project_json["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
        "Project creation failed: {create_project_res:?}"
    );

    kamu_api_server_client
        .account()
        .set_account_quota_bytes(ACCOUNT, 1)
        .await;

    let announcement_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(CREATE_ANNOUNCEMENT).variables(Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "headline": "quota",
                "body": "exceeded",
                "attachments": [],
                "accessLevel": "public",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "categories": ["news"],
                "tags": ["quota"],
            }))),
        )
        .await;

    assert!(
        announcement_res.errors.is_empty(),
        "Announcement mutation failed: {announcement_res:?}"
    );
    let payload = announcement_res.data.into_json().unwrap_or_default()["molecule"]["v2"]
        ["project"]["announcements"]["create"]
        .clone();
    assert_eq!(payload["isSuccess"], json!(false), "{payload:?}");
    let msg = payload["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("Quota exceeded"),
        "unexpected message: {payload:?}"
    );
    // Announcement mutation surfaces a specific error union variant
    assert_eq!(
        payload["__typename"],
        json!("CreateAnnouncementErrorQuotaExceeded"),
        "{payload:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_molecule_v2_activity_change_by_for_remove(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    const USER_ACCOUNT: &str = "molecule";
    const USER_PASSWORD: &str = "molecule.dev";
    const USER_DID: &str = "did:ethr:0xE2eActivityUser";
    const ADMIN_DID: &str = "did:ethr:0xE2eActivityAdmin";

    const IPNFT_ADDRESS: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1";
    const IPNFT_TOKEN_ID: &str = "1300";
    let ipnft_uid = format!("{IPNFT_ADDRESS}_{IPNFT_TOKEN_ID}");

    let path = "/activity-entry.txt";
    let tag = "e2e-activity-tag";
    let category = "e2e-activity-category";

    kamu_api_server_client
        .auth()
        .login_with_password(USER_ACCOUNT, USER_PASSWORD)
        .await;

    // Create project
    let create_project_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(CREATE_PROJECT).variables(Variables::from_json(json!({
                "ipnftSymbol": "activity-remove-e2e",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": IPNFT_ADDRESS,
                "ipnftTokenId": IPNFT_TOKEN_ID,
            }))),
        )
        .await;
    assert!(
        create_project_res.errors.is_empty(),
        "Project creation failed: {create_project_res:?}"
    );
    let create_payload = create_project_res
        .data
        .clone()
        .into_json()
        .unwrap_or_default();
    assert_eq!(
        create_payload["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
        "{create_payload:?}"
    );

    // Upload initial file (change_by = user)
    let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(b"hello world");
    let upload_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(CREATE_VERSIONED_FILE).variables(Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": path,
                "content": encoded,
                "contentType": "text/plain",
                "changeBy": USER_DID,
                "accessLevel": "public",
                "description": "initial",
                "categories": [category],
                "tags": [tag],
                "contentText": "hello world",
                "encryptionMetadata": Value::Null,
            }))),
        )
        .await;
    assert!(
        upload_res.errors.is_empty(),
        "Upload mutation failed: {upload_res:?}"
    );
    let upload_json = upload_res.data.into_json().unwrap_or_default();
    let file_ref = upload_json["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .expect("ref missing")
        .to_string();

    // Update metadata (change_by = user)
    let update_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(UPDATE_METADATA_QUERY).variables(Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": file_ref,
                "expectedHead": Value::Null,
                "changeBy": USER_DID,
                "accessLevel": "public",
                "description": "updated description",
                "categories": [category],
                "tags": [tag],
                "contentText": "updated",
            }))),
        )
        .await;
    assert!(
        update_res.errors.is_empty(),
        "Update metadata failed: {update_res:?}"
    );
    let update_json = update_res.data.into_json().unwrap_or_default();
    assert_eq!(
        update_json["molecule"]["v2"]["project"]["dataRoom"]["updateFileMetadata"]["isSuccess"],
        json!(true),
        "{update_json:?}"
    );

    // Remove entry with admin changeBy (still authorized as project owner)
    let remove_res = kamu_api_server_client
        .graphql_api_call_ex(
            Request::new(REMOVE_ENTRY_QUERY).variables(Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": path,
                "changeBy": ADMIN_DID,
            }))),
        )
        .await;
    assert!(
        remove_res.errors.is_empty(),
        "Remove entry failed: {remove_res:?}"
    );
    let remove_json = remove_res.data.into_json().unwrap_or_default();
    assert_eq!(
        remove_json["molecule"]["v2"]["project"]["dataRoom"]["removeEntry"]["isSuccess"],
        json!(true),
        "{remove_json:?}"
    );

    // Project activity (should surface admin changeBy on remove)
    // Retrying due to eventual consistency of ES indexing
    let retry_strategy = FixedInterval::from_millis(5_000).take(6); // 30s
    Retry::spawn(retry_strategy, || async {
        let project_activity = kamu_api_server_client
            .graphql_api_call_ex(Request::new(LIST_PROJECT_ACTIVITY_QUERY).variables(
                Variables::from_json(json!({
                    "ipnftUid": ipnft_uid,
                    "filters": {
                        "byTags": [tag],
                    },
                })),
            ))
            .await;
        assert!(
            project_activity.errors.is_empty(),
            "Project activity query failed: {project_activity:?}"
        );
        let project_nodes = project_activity.data.into_json().unwrap_or_default()["molecule"]["v2"]
            ["project"]["activity"]["nodes"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        if project_nodes.len() < 3 {
            return Err(());
        }

        assert_eq!(
            project_nodes.len(),
            3,
            "Unexpected project activity: {project_nodes:?}"
        );
        assert_eq!(
            project_nodes[0]["__typename"],
            json!("MoleculeActivityFileRemovedV2"),
            "{project_nodes:?}"
        );
        assert_eq!(
            project_nodes[0]["entry"]["changeBy"],
            json!(ADMIN_DID),
            "Project remove changeBy mismatch: {project_nodes:?}"
        );
        assert_eq!(
            project_nodes[1]["entry"]["changeBy"],
            json!(USER_DID),
            "Update changeBy mismatch: {project_nodes:?}"
        );
        assert_eq!(
            project_nodes[2]["entry"]["changeBy"],
            json!(USER_DID),
            "Add changeBy mismatch: {project_nodes:?}"
        );

        Ok::<(), ()>(())
    })
    .await
    .unwrap();

    // Global activity should mirror the same changeBy values
    // Retrying due to eventual consistency of ES indexing
    let retry_strategy = FixedInterval::from_millis(5_000).take(6); // 30s
    Retry::spawn(retry_strategy, || async {
        let global_activity = kamu_api_server_client
            .graphql_api_call_ex(Request::new(LIST_GLOBAL_ACTIVITY_QUERY).variables(
                Variables::from_json(json!({
                    "filters": {
                        "byTags": [tag],
                    },
                })),
            ))
            .await;
        assert!(
            global_activity.errors.is_empty(),
            "Global activity query failed: {global_activity:?}"
        );
        let global_nodes = global_activity.data.into_json().unwrap_or_default()["molecule"]["v2"]
            ["activity"]["nodes"]
            .as_array()
            .cloned()
            .unwrap_or_default();

        if global_nodes.len() < 3 {
            return Err(());
        }
        assert_eq!(
            global_nodes.len(),
            3,
            "Unexpected global activity: {global_nodes:?}"
        );
        assert_eq!(
            global_nodes[0]["__typename"],
            json!("MoleculeActivityFileRemovedV2"),
            "{global_nodes:?}"
        );
        assert_eq!(
            global_nodes[0]["entry"]["changeBy"],
            json!(ADMIN_DID),
            "Global remove changeBy mismatch: {global_nodes:?}"
        );
        assert_eq!(
            global_nodes[1]["entry"]["changeBy"],
            json!(USER_DID),
            "Global update changeBy mismatch: {global_nodes:?}"
        );
        assert_eq!(
            global_nodes[2]["entry"]["changeBy"],
            json!(USER_DID),
            "Global add changeBy mismatch: {global_nodes:?}"
        );

        Ok::<(), ()>(())
    })
    .await
    .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Queries
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
                }
            }
        }
    }
    "#
);

const DATA_ROOM_UPLOAD: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $path: CollectionPath!, $content: Base64Usnp!) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            dataRoom {
              uploadFile(
                path: $path
                content: $content
                contentType: "text/plain"
                changeBy: "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC"
                accessLevel: "public"
              ) {
                isSuccess
                message
                __typename
              }
            }
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
                  accessLevel
                  changeBy
                }
              }
              ... on MoleculeActivityFileUpdatedV2 {
                __typename
                entry {
                  path
                  ref
                  accessLevel
                  changeBy
                }
              }
              ... on MoleculeActivityFileRemovedV2 {
                __typename
                entry {
                  path
                  ref
                  accessLevel
                  changeBy
                }
              }
              ... on MoleculeActivityAnnouncementV2 {
                __typename
                announcement {
                  id
                  headline
                  body
                  attachments {
                    path
                    ref
                  }
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
                    accessLevel
                    changeBy
                  }
                }
                ... on MoleculeActivityFileUpdatedV2 {
                  __typename
                  entry {
                    path
                    ref
                    accessLevel
                    changeBy
                  }
                }
                ... on MoleculeActivityFileRemovedV2 {
                  __typename
                  entry {
                    path
                    ref
                    accessLevel
                    changeBy
                  }
                }
                ... on MoleculeActivityAnnouncementV2 {
                  __typename
                  announcement {
                    id
                    headline
                    body
                    attachments {
                      path
                      ref
                    }
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

const CREATE_ANNOUNCEMENT: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $headline: String!, $body: String!, $attachments: [DatasetID!], $accessLevel: String!, $changeBy: String!, $categories: [String!]!, $tags: [String!]!) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            announcements {
              create(
                headline: $headline
                body: $body
                attachments: $attachments
                accessLevel: $accessLevel
                changeBy: $changeBy
                categories: $categories
                tags: $tags
              ) {
                isSuccess
                message
                __typename
              }
            }
          }
        }
      }
    }
    "#
);

const CREATE_VERSIONED_FILE: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $path: CollectionPathV2!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String, $encryptionMetadata: MoleculeEncryptionMetadataInput) {
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
                    path
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

const UPDATE_METADATA_QUERY: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $ref: DatasetID!, $expectedHead: String, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            dataRoom {
              updateFileMetadata(
                ref: $ref
                expectedHead: $expectedHead
                accessLevel: $accessLevel
                changeBy: $changeBy
                description: $description
                categories: $categories
                tags: $tags
                contentText: $contentText
              ) {
                isSuccess
                message
                __typename
                ... on UpdateVersionErrorCasFailed {
                    expectedHead
                    actualHead
                }
              }
            }
          }
        }
      }
    }
    "#
);

const REMOVE_ENTRY_QUERY: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $path: CollectionPathV2!, $changeBy: String!) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            dataRoom {
              removeEntry(path: $path, changeBy: $changeBy) {
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
