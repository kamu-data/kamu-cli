// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_dataset_adapter_versioned_file_direct_upload_download_mt(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    const DUMMY_FILE_NAME: &str = "dummy.txt";
    const DUMMY_FILE_CONTENT: &str = "It's certainly not a waste of space in the S3 bucket.";

    kamu_api_server_client.auth().login_as_e2e_user().await;

    // Create a versioned file dataset
    let res = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
                mutation createFile(
                  $datasetAlias: DatasetAlias!,
                  $datasetVisibility: DatasetVisibility!,
                  $extraColumns: [ColumnInput!],
                ) {
                  datasets {
                    createVersionedFile(
                      datasetAlias: $datasetAlias,
                      datasetVisibility: $datasetVisibility,
                      extraColumns: $extraColumns,
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
            ),
            Some(json!({
              "datasetAlias": "project-overview",
              "datasetVisibility": "PRIVATE",
              "extraColumns": [
                {"name": "access_level", "type": {"ddl": "string"}},
                {"name": "change_by", "type": {"ddl": "string"}}
              ]
            })),
        )
        .await
        .unwrap();

    let did = res["datasets"]["createVersionedFile"]["dataset"]["id"]
        .as_str()
        .unwrap();

    // Initiate direct upload
    let res = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
                mutation startUpload(
                  $datasetId: DatasetID!,
                  $contentType: String,
                  $contentLength: Int!,
                ) {
                  datasets {
                    byId(datasetId: $datasetId) {
                      asVersionedFile {
                        startUploadNewVersion(
                          contentType: $contentType,
                          contentLength: $contentLength,
                        ) {
                          isSuccess
                          message
                          ... on StartUploadVersionSuccess {
                            method
                            url
                            headers {
                              key
                              value
                            }
                            uploadToken
                            useMultipart
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ),
            Some(json!({
              "datasetId": did,
               "contentLength": DUMMY_FILE_CONTENT.len(),
              "contentType": "text/plain"
            })),
        )
        .await
        .unwrap();

    let ctx = &res["datasets"]["byId"]["asVersionedFile"]["startUploadNewVersion"];
    let upload_token = ctx["uploadToken"].as_str().unwrap();

    // Upload file
    let upload_context = kamu_adapter_http::platform::UploadContext {
        upload_url: ctx["url"].as_str().unwrap().to_string(),
        method: ctx["method"].as_str().unwrap().to_string(),
        use_multipart: ctx["useMultipart"].as_bool().unwrap(),
        headers: ctx["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| {
                (
                    h["key"].as_str().unwrap().to_string(),
                    h["value"].as_str().unwrap().to_string(),
                )
            })
            .collect(),
        fields: Vec::new(),
        upload_token: upload_token.parse().unwrap(),
    };

    kamu_api_server_client
        .upload()
        .upload_file(&upload_context, DUMMY_FILE_NAME, DUMMY_FILE_CONTENT)
        .await
        .unwrap();

    // Finalize direct upload
    let res = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
                mutation finishUpload(
                  $datasetId: DatasetID!,
                  $uploadToken: String!,
                  $extraData: JSON,
                  $expectedHead: Multihash,
                ) {
                  datasets {
                    byId(datasetId: $datasetId) {
                      asVersionedFile {
                        finishUploadNewVersion(
                          uploadToken: $uploadToken,
                          extraData: $extraData,
                          expectedHead: $expectedHead,
                        ) {
                          isSuccess
                          message
                          ... on UpdateVersionSuccess {
                            newVersion
                            contentHash
                          }
                        }
                      }
                    }
                  }
                }
                "#
            ),
            Some(json!({
              "datasetId": did,
              "uploadToken": upload_token,
              "extraData": {
                "access_level": "public",
                "change_by": "did:ethr:0x41f5F090af7fF6385d0EfD64c4254B6645fE75BC"
              },
            })),
        )
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        res["datasets"]["byId"]["asVersionedFile"]["finishUploadNewVersion"],
        json!({
          "isSuccess": true,
          "message": "",
          "newVersion": 1,
          "contentHash": "f16208e1d101a0464b9312a75a8a455cf81b81d92ca265285e61047a35bbc40577e4b",
        }),
    );

    // Read back the content
    // TODO: Figure out how to test direct download
    let res = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
                query directDownloadUrl($datasetId: DatasetID!) {
                  datasets {
                    byId(datasetId: $datasetId) {
                      head
                      asVersionedFile {
                        latest {
                          version
                          contentType
                          contentHash
                          extraData
                          content
                        }
                      }
                    }
                  }
                }
                "#
            ),
            Some(json!({
              "datasetId": did,
            })),
        )
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        res["datasets"]["byId"]["asVersionedFile"]["latest"],
        json!({
          "version": 1,
          "contentType": "text/plain",
          "contentHash": "f16208e1d101a0464b9312a75a8a455cf81b81d92ca265285e61047a35bbc40577e4b",
          "content": "SXQncyBjZXJ0YWlubHkgbm90IGEgd2FzdGUgb2Ygc3BhY2UgaW4gdGhlIFMzIGJ1Y2tldC4",
          "extraData": {
            "access_level": "public",
            "change_by": "did:ethr:0x41f5F090af7fF6385d0EfD64c4254B6645fE75BC"
          }
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
