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
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MULTITENANT_MOLECULE_QUOTA_CONFIG: &str = indoc::indoc!(
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
