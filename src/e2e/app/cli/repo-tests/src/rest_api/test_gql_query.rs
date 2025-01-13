// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::MetadataFactory;
use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use opendatafabric::serde::yaml::YamlDatasetSnapshotSerializer;
use opendatafabric::serde::DatasetSnapshotSerializer;
use opendatafabric::DatasetKind;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_empty_returns_correct_alias_mt(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createEmpty(datasetKind: ROOT, datasetAlias: "empty-root-dataset") {
                      message
                      ... on CreateDatasetResultSuccess {
                        dataset {
                          alias
                          owner {
                            accountName
                          }
                        }
                      }
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createEmpty": {
                      "dataset": {
                        "alias": "e2e-user/empty-root-dataset",
                        "owner": {
                          "accountName": "e2e-user"
                        }
                      },
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_empty_returns_correct_alias_st(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_kamu().await;

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createEmpty(datasetKind: ROOT, datasetAlias: "empty-root-dataset") {
                      message
                      ... on CreateDatasetResultSuccess {
                        dataset {
                          alias
                          owner {
                            accountName
                          }
                        }
                      }
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createEmpty": {
                      "dataset": {
                        "alias": "empty-root-dataset",
                        "owner": {
                          "accountName": "kamu"
                        }
                      },
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_from_snapshot_returns_correct_alias_mt(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_yaml = String::from_utf8_lossy(
        &YamlDatasetSnapshotSerializer
            .write_manifest(&snapshot)
            .unwrap(),
    )
    .to_string();

    let request_code = indoc::indoc!(
        r#"
        mutation {
          datasets {
            createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML) {
              ... on CreateDatasetResultSuccess {
                dataset {
                  name
                  kind
                  alias
                  owner {
                    accountName
                  }
                }
              }
            }
          }
        }
        "#
    )
    .replace("<content>", &snapshot_yaml.escape_default().to_string());

    kamu_api_server_client
        .graphql_api_call_assert(
            &request_code,
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createFromSnapshot": {
                      "dataset": {
                        "alias": "e2e-user/foo",
                        "kind": "ROOT",
                        "name": "foo",
                        "owner": {
                          "accountName": "e2e-user"
                        }
                      }
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_from_snapshot_returns_correct_alias_st(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_yaml = String::from_utf8_lossy(
        &YamlDatasetSnapshotSerializer
            .write_manifest(&snapshot)
            .unwrap(),
    )
    .to_string();

    let request_code = indoc::indoc!(
        r#"
        mutation {
          datasets {
            createFromSnapshot (snapshot: "<content>", snapshotFormat: YAML) {
              ... on CreateDatasetResultSuccess {
                dataset {
                  name
                  kind
                  alias
                  owner {
                    accountName
                  }
                }
              }
            }
          }
        }
        "#
    )
    .replace("<content>", &snapshot_yaml.escape_default().to_string());

    kamu_api_server_client
        .graphql_api_call_assert(
            &request_code,
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createFromSnapshot": {
                      "dataset": {
                        "alias": "foo",
                        "kind": "ROOT",
                        "name": "foo",
                        "owner": {
                          "accountName": "kamu"
                        }
                      }
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
