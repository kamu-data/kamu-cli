// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::value;
use base64::Engine as _;
use bon::bon;
use chrono::Utc;
use indoc::indoc;
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    DatasetRegistry,
    DatasetRegistryExt,
};
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use kamu_molecule_domain::MoleculeCreateProjectUseCase;
use kamu_search::*;
use kamu_search_cache_inmem::InMemoryEmbeddingsCacheRepository;
use kamu_search_elasticsearch::testing::{ElasticsearchBaseHarness, ElasticsearchTestContext};
use kamu_search_services::{DummyEmbeddingsEncoder, EmbeddingsProviderImpl};
use messaging_outbox::{OutboxAgent, OutboxProvider};
use num_bigint::BigInt;
use odf::dataset::MetadataChainExt;
use serde_json::json;
use time_source::SystemTimeSourceProvider;

use crate::utils::{
    AuthenticationCatalogsResult,
    BaseGQLDatasetHarness,
    GraphQLQueryRequest,
    PredefinedAccountOpts,
    authentication_catalogs_ext,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_gql_custom_molecule {
    ($test_name: expr) => {
        ::paste::paste! {
            #[::test_group::group(elasticsearch)]
            #[::test_log::test(::kamu_search_elasticsearch::test)]
            async fn [<$test_name "_es">](es_ctx: Arc<ElasticsearchTestContext>) {
                let search_variant = GraphQLMoleculeV2HarnessSearchVariant::ElasticsearchBased(es_ctx);
                $test_name(search_variant).await;
            }

            #[::test_log::test(::tokio::test)]
            async fn [<$test_name "_src">]() {
                let search_variant = GraphQLMoleculeV2HarnessSearchVariant::SourceBased;
                $test_name(search_variant).await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const USER_1: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC";
const USER_2: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CREATE_PROJECT: &str = indoc!(
    r#"
    mutation ($ipnftSymbol: String!, $ipnftUid: String!, $ipnftAddress: String!, $ipnftTokenId: Int!) {
      molecule {
        v2 {
          createProject(
            ipnftSymbol: $ipnftSymbol
            ipnftUid: $ipnftUid
            ipnftAddress: $ipnftAddress
            ipnftTokenId: $ipnftTokenId
          ) {
            isSuccess
            message
            __typename
            ... on CreateProjectSuccess {
              project {
                account {
                  id
                  accountName
                }
                ipnftUid
                dataRoom {
                  dataset {
                    id
                    alias
                  }
                }
                announcements {
                  dataset {
                    id
                    alias
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
// TODO: find a way to output tags/categories
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
const MOVE_ENTRY_QUERY: &str = indoc!(
    r#"
    mutation ($ipnftUid: String!, $fromPath: CollectionPathV2!, $toPath: CollectionPathV2!, $changeBy: String!) {
      molecule {
        v2 {
          project(ipnftUid: $ipnftUid) {
            dataRoom {
              moveEntry(fromPath: $fromPath, toPath: $toPath, changeBy: $changeBy) {
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
const SEARCH_QUERY: &str = indoc!(
    r#"
    query ($prompt: String!, $filters: MoleculeSemanticSearchFilters) {
      molecule {
        v2 {
          search(prompt: $prompt, filters: $filters) {
            totalCount
            nodes {
              ... on MoleculeSemanticSearchFoundDataRoomEntry {
                __typename
                entry {
                  project {
                    ipnftUid
                  }
                  asDataset {
                    id
                  }
                  path
                  ref
                  changeBy
                  accessLevel
                  asVersionedFile {
                    matching {
                      version
                      contentType
                      accessLevel
                      changeBy
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
              ... on MoleculeSemanticSearchFoundAnnouncement {
                __typename
                announcement {
                  project {
                    ipnftUid
                  }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_provision_project);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_disable_enable_project);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_disable_enable_project_errors);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_cannot_recreate_disabled_project_with_same_symbol);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_data_room_quota_exceeded);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_data_room_operations);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_data_room_as_of_block_hash);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_announcements_quota_exceeded);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_announcements_operations);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_activity);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_activity_access_level_rules_filters);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_gql_custom_molecule!(test_molecule_v2_search);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(resourcegen)]
#[test_log::test(tokio::test)]
async fn test_molecule_v2_dump_dataset_snapshots_src() {
    // NOTE: Instead of dumping the source snapshots of datasets here we create
    // datasets all and scan their metadata chains to dump events exactly how they
    // appear in real datasets

    const PROJECT_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(GraphQLMoleculeV2HarnessSearchVariant::SourceBased)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let dataset_reg = harness
        .catalog_authorized
        .get_one::<dyn kamu_datasets::DatasetRegistry>()
        .unwrap();

    let (project_data_room_dataset, project_announcements_dataset) = {
        let res = harness
            .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
                async_graphql::Variables::from_json(json!({
                    "ipnftSymbol": "VITAFAST",
                    "ipnftUid": PROJECT_UID,
                    "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                    "ipnftTokenId": "9",
                })),
            ))
            .await;

        let data = res.data.into_json().unwrap();

        let data_room_dataset = dataset_reg
            .get_dataset_by_id(
                &odf::DatasetID::from_did_str(
                    data["molecule"]["v2"]["createProject"]["project"]["dataRoom"]["dataset"]["id"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let announcements_dataset = dataset_reg
            .get_dataset_by_id(
                &odf::DatasetID::from_did_str(
                    data["molecule"]["v2"]["createProject"]["project"]["announcements"]["dataset"]
                        ["id"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        (data_room_dataset, announcements_dataset)
    };

    let project_file_dataset = {
        let data = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello foo"),
                "contentType": "text/plain",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "accessLevel": "public",
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        dataset_reg
            .get_dataset_by_id(
                &odf::DatasetID::from_did_str(
                    data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]["ref"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap()
    };

    // Create an announcement to force creation of global announcements dataset
    GraphQLQueryRequest::new(
        CREATE_ANNOUNCEMENT,
        async_graphql::Variables::from_value(value!({
            "ipnftUid": PROJECT_UID,
            "headline": "Test announcement",
            "body": "Blah blah",
            "attachments": [],
            "accessLevel": "holders",
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            "categories": [],
            "tags": [],
        })),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await
    .into_result()
    .unwrap();

    let molecule_projects_dataset = dataset_reg
        .get_dataset_by_ref(&"molecule/projects".parse().unwrap())
        .await
        .unwrap();

    let molecule_data_room_activity_dataset = dataset_reg
        .get_dataset_by_ref(&"molecule/data-room-activity".parse().unwrap())
        .await
        .unwrap();

    let molecule_announcements_dataset = dataset_reg
        .get_dataset_by_ref(&"molecule/announcements".parse().unwrap())
        .await
        .unwrap();

    // NOTE: Dataset names in snapshots must match dataset names
    //       -- we use them for data migration
    //       -->
    dump_snapshot(
        molecule_projects_dataset.as_ref(),
        "molecule-projects",
        Some("projects"),
    )
    .await;
    dump_snapshot(
        molecule_announcements_dataset.as_ref(),
        "molecule-announcements",
        Some("announcements"),
    )
    .await;
    dump_snapshot(
        molecule_data_room_activity_dataset.as_ref(),
        "molecule-data-room-activity",
        Some("data-room-activity"),
    )
    .await;
    // <--
    dump_snapshot(
        project_data_room_dataset.as_ref(),
        "project-data-room",
        None,
    )
    .await;
    dump_snapshot(
        project_announcements_dataset.as_ref(),
        "project-announcements",
        None,
    )
    .await;
    dump_snapshot(project_file_dataset.as_ref(), "project-file", None).await;
}

async fn dump_snapshot(
    dataset: &dyn odf::dataset::Dataset,
    file_name: &str,
    dataset_name_override: Option<&str>,
) {
    use futures::TryStreamExt;

    let blocks: Vec<_> = dataset
        .as_metadata_chain()
        .iter_blocks()
        .try_collect()
        .await
        .unwrap();

    let snapshot = odf::DatasetSnapshot {
        name: dataset_name_override.unwrap_or(file_name).parse().unwrap(),
        kind: odf::DatasetKind::Root,
        metadata: blocks
            .into_iter()
            .rev()
            .map(|(_, b)| b.event)
            .filter(|e| {
                !matches!(
                    e,
                    odf::MetadataEvent::Seed(_) | odf::MetadataEvent::AddData(_)
                )
            })
            .collect(),
    };

    let yaml = odf::serde::yaml::YamlDatasetSnapshotSerializer
        .write_manifest_str(&snapshot)
        .unwrap();

    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(format!("../../../resources/molecule/{file_name}.yaml"));

    std::fs::write(path, &yaml).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_provision_project(search_variant: GraphQLMoleculeV2HarnessSearchVariant) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
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
    pretty_assertions::assert_eq!(
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
    let data_room_did = res["project"]["dataRoom"]["dataset"]["id"]
        .as_str()
        .unwrap();
    let announcements_did = res["project"]["announcements"]["dataset"]["id"]
        .as_str()
        .unwrap();
    assert_ne!(project_account_id, harness.molecule_account_id.to_string());
    pretty_assertions::assert_eq!(project_account_name, "molecule.vitafast");
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
                    "dataset": {
                        "id": data_room_did,
                        "alias": format!("{project_account_name}/data-room"),
                    }
                },
                "announcements": {
                    "dataset": {
                        "id": announcements_did,
                        "alias": format!("{project_account_name}/announcements"),
                    },
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
    pretty_assertions::assert_eq!(
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

    // Read back the project entry by `ipnftUid`` with modified character casing
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
                "ipnftUid": "0xcad88677cA87a7815728C72D74B4ff4982d54fc1_9",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"]["isSuccess"],
        json!(true),
    );

    // Both projects appear in the list
    let res = harness
        .execute_authorized_query(async_graphql::Request::new(LIST_PROJECTS))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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

async fn test_molecule_v2_disable_enable_project(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
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
            Some(Utc::now()),
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
        harness: &GraphQLMoleculeV2Harness,
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(disable_error, format!("Project {ipnft_uid} not found"));

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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([]),
    );

    // Querying by UID returns None
    let res = query_project(&harness, ipnft_uid).await;
    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
        pretty_assertions::assert_eq!(
            enable_res["molecule"]["v2"]["enableProject"]["project"],
            json!({ "__typename": "MoleculeProjectMutV2" }),
        );
    }

    let post_enable_chain_len = harness
        .projects_metadata_chain_len(&projects_dataset_alias)
        .await;
    // it should be two blocks longer than initial (one disable and one enable
    // operations)
    pretty_assertions::assert_eq!(post_enable_chain_len, initial_chain_len + 2);

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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["projects"]["nodes"],
        json!([{
            "ipnftUid": ipnft_uid,
        }]),
    );

    let res = query_project(&harness, ipnft_uid).await;
    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"],
        json!({
            "ipnftUid": ipnft_uid,
            "ipnftSymbol": "vitafast",
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_disable_enable_project_errors(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
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
    pretty_assertions::assert_eq!(disable_error, format!("Project {missing_uid} not found"));

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
    pretty_assertions::assert_eq!(
        enable_error,
        format!("No historical entries for project {missing_uid}")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_cannot_recreate_disabled_project_with_same_symbol(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["createProject"],
        json!({
            "isSuccess": false,
            "message": "Conflict with existing project vitafast (0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9)",
            "__typename": "CreateProjectErrorConflict",
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_data_room_quota_exceeded(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_901";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_account_opts(PredefinedAccountOpts {
            is_admin: true,
            ..Default::default()
        })
        .build()
        .await;

    // Create project
    let create_project_res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "quota-room",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "901",
            })),
        ))
        .await;
    assert!(create_project_res.is_ok(), "{create_project_res:#?}");

    // Cap storage to force quota failure
    let set_quota_res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
                r#"
            mutation ($limit: Int!) {
              accounts {
                me {
                  quotas {
                    setAccountQuotas(quotas: { storage: { limitTotalBytes: $limit } }) {
                      isSuccess
                    }
                  }
                }
              }
            }
            "#
            ))
            .variables(async_graphql::Variables::from_json(json!({ "limit": 1 }))),
        )
        .await;
    assert!(set_quota_res.is_ok(), "{set_quota_res:#?}");

    // Try uploading a file - should fail with quota exceeded
    let upload_res = harness
        .execute_authorized_query(
            async_graphql::Request::new(indoc!(
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
                            ... on MoleculeQuotaExceeded {
                              used
                              incoming
                              limit
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
                "path": "/quota.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
            }))),
        )
        .await;

    let payload = upload_res.data.into_json().unwrap()["molecule"]["v2"]["project"]["dataRoom"]
        ["uploadFile"]
        .clone();
    pretty_assertions::assert_eq!(payload["isSuccess"], json!(false));
    let msg = payload["message"].as_str().unwrap();
    assert!(msg.contains("Quota exceeded"), "unexpected message: {msg}");
    pretty_assertions::assert_eq!(payload["__typename"], json!("MoleculeQuotaExceeded"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_data_room_operations(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
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
    pretty_assertions::assert_eq!(
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
    const UPLOAD_WITH_ALL_METHOD_ARGUMENTS_PROVIDED: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $path: CollectionPathV2!, $content: Base64Usnp!, $contentType: String!, $changeBy: String!, $accessLevel: String!, $description: String!, $categories: [String!]!, $tags: [String!]!, $contentText: String!, $encryptionMetadata: MoleculeEncryptionMetadataInput!) {
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
                    __typename
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
                    ... on MoleculeDataRoomPathOccupied {
                      byEntry {
                        project {
                          ipnftUid
                        }
                        ref
                        changeBy
                        accessLevel
                        asVersionedFile {
                          matching {
                            version
                            contentHash
                            contentLength
                            contentType
                            accessLevel
                            changeBy
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
    );

    let res = GraphQLQueryRequest::new(
        UPLOAD_WITH_ALL_METHOD_ARGUMENTS_PROVIDED,
        async_graphql::Variables::from_value(value!({
            "ipnftUid": ipnft_uid,
            "path": "/foo.txt",
            "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
            "contentType": "text/plain",
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            "accessLevel": "public",
            "description": "Plain text file",
            "categories": ["test-category-1"],
            "tags": ["test-tag1"],
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
        })),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;

    let res_data = res.data.into_json().unwrap();
    pretty_assertions::assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_1_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    pretty_assertions::assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"],
        json!({
            "__typename": "MoleculeDataRoomFinishUploadFileResultSuccess",
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
                        "categories": ["test-category-1"],
                        "tags": ["test-tag1"],
                    }
                }
            }
        })
    );

    // Attempt to upload a new file to the same path
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_WITH_ALL_METHOD_ARGUMENTS_PROVIDED,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello 2"),
                "contentType": "application/octet-stream",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                "accessLevel": "holders",
                "description": "Plain text file -- the second attempt",
                "categories": ["test-category-4"],
                "tags": ["test-tag5"],
                "contentText": "hello -- the second attempt",
                "encryptionMetadata": {
                    "dataToEncryptHash": "EM1 -- the second attempt",
                    "accessControlConditions": "EM2 -- the second attempt",
                    "encryptedBy": "EM3 -- the second attempt",
                    "encryptedAt": "EM4 -- the second attempt",
                    "chain": "EM5 -- the second attempt",
                    "litSdkVersion": "EM6 -- the second attempt",
                    "litNetwork": "EM7 -- the second attempt",
                    "templateName": "EM8 -- the second attempt",
                    "contractVersion": "EM9 -- the second attempt",
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
                            "uploadFile": {
                                "__typename": "MoleculeDataRoomPathOccupied",
                                "isSuccess": false,
                                "message": "Path is occupied",
                                "byEntry": {
                                    "project": {
                                        "ipnftUid": ipnft_uid,
                                    },
                                    "ref": file_1_did,
                                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                                    "accessLevel": "public",
                                    "asVersionedFile": {
                                        "matching": {
                                            "version": 1,
                                            "contentHash": "f16203338be694f50c5f338814986cdf0686453a888b84f424d792af4b9202398f392",
                                            "contentLength": 5,
                                            "contentType": "text/plain",
                                            "accessLevel": "public",
                                            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                                            "description": "Plain text file",
                                            "categories": ["test-category-1"],
                                            "tags": ["test-tag1"],
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
                                            "content": "aGVsbG8",
                                        }
                                    },
                                },
                            }
                        }
                    }
                }
            }
        })
    );

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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
                "categories": ["test-category-2"],
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
    pretty_assertions::assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_2_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    pretty_assertions::assert_eq!(
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
                        "categories": ["test-category-2"],
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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
                "accessLevel": "holders",
            }))),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    pretty_assertions::assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_3_did: &str = res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    pretty_assertions::assert_eq!(
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
                        "accessLevel": "holders",
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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
        query ($ipnftUid: String!, $filters: MoleculeDataRoomEntriesFilters) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  latest {
                    entries(filters: $filters) {
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
                "filters": null,
            })),
        ))
        .await;

    assert!(res.is_ok(), "{res:#?}");
    pretty_assertions::assert_eq!(
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
                            "categories": ["test-category-2"],
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
                            "accessLevel": "holders",
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
                            "categories": ["test-category-1"],
                            "tags": ["test-tag1"],
                        }
                    }
                },
            ],
        })
    );

    // First, we try to update using a non-existent ref

    const UPDATE_FILE_BY_REF_QUERY: &str = indoc!(
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
    );

    let random_ref = odf::DatasetID::new_generated_ed25519().1;
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_FILE_BY_REF_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "ref": random_ref,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye -- random ref"),
                "contentType": "application/octet-stream",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC -- random ref",
                "accessLevel": "public",
                "description": "Plain text file that was updated",
                "categories": ["test-category-5"],
                "tags": ["test-tag6"],
                "contentText": "bye -- random ref",
                "encryptionMetadata": null,
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
                            "uploadFile": {
                                "isSuccess": false,
                                "message": "Data room entry not found by ref",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actually upload a new version of an existing file

    let res = harness
        .execute_authorized_query(
            async_graphql::Request::new(UPDATE_FILE_BY_REF_QUERY).variables(
                async_graphql::Variables::from_json(json!({
                    "ipnftUid": ipnft_uid,
                    "ref": file_1_did,
                    "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye"),
                    "contentType": "text/plain",
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    "accessLevel": "public",
                    "description": "Plain text file that was updated",
                    "categories": ["test-category-1", "test-category-3"],
                    "tags": ["test-tag1", "test-tag4"],
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
                })),
            ),
        )
        .await;

    assert!(res.is_ok(), "{res:#?}");
    let res_data = res.data.into_json().unwrap();
    pretty_assertions::assert_eq!(
        res_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );

    pretty_assertions::assert_eq!(
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
                        "categories": ["test-category-1", "test-category-3"],
                        "tags": ["test-tag1", "test-tag4"],
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
                    "categories": ["test-category-1", "test-category-3"],
                    "tags": ["test-tag1", "test-tag4"],
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
    pretty_assertions::assert_eq!(
        res.data.into_json().unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entry"],
        json!({
            "path": "/baz.txt",
            "ref": file_3_did,
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            "asVersionedFile": {
                "latest": {
                    "version": 1,
                    "accessLevel": "holders",
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
    // Non-existent file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "fromPath": "/non-existent-path.txt",
                "toPath": "/2025/foo.txt",
                "changeBy": USER_1,
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
                                "message": "Data room entry not found by path",
                            }
                        }
                    }
                }
            }
        })
    );

    // Attempt to move to an already occupied path
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "fromPath": "/foo.txt",
                "toPath": "/baz.txt",
                "changeBy": USER_1,
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
                                "message": "Path is occupied",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual file move
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "fromPath": "/foo.txt",
                "toPath": "/2025/foo.txt",
                "changeBy": USER_1,
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

    harness.synchronize_agents().await;

    let all_entries_nodes = json!([
        {
            "path": "/2025/foo.txt",
            "ref": file_1_did,
            "changeBy": USER_1,
            "asVersionedFile": {
                "latest": {
                    "accessLevel": "public",
                    "contentType": "text/plain",
                    "version": 2,
                    "description": "Plain text file that was updated",
                    "categories": ["test-category-1", "test-category-3"],
                    "tags": ["test-tag1", "test-tag4"],
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
                    "categories": ["test-category-2"],
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
                    "accessLevel": "holders",
                    "contentType": "text/plain",
                    "version": 1,
                    "description": null,
                    "categories": [],
                    "tags": [],
                }
            }
        },
    ]);
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 3,
            "nodes": all_entries_nodes,
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
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    /////////////
    // Filters //
    /////////////

    // Filters without values
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 3,
            "nodes": all_entries_nodes,
        })
    );

    // Filters by tags: [test-tag4]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": ["test-tag4"],
                    "byCategories": null,
                    "byAccessLevels": null,
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
                        }
                    }
                },
                // {
                //     "path": "/bar.txt",
                //     "ref": file_2_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": "Plain text file",
                //             "categories": ["test-category-2"],
                //             "tags": ["test-tag1", "test-tag2"],
                //         }
                //     }
                // },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by tags: [test-tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": ["test-tag1", "test-tag1"],
                    "byCategories": null,
                    "byAccessLevels": null,
                },
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
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
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
                            "categories": ["test-category-2"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by tags: [test-tag2, test-tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": ["test-tag2", "test-tag1"],
                    "byCategories": null,
                    "byAccessLevels": null,
                },
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
                     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                     "asVersionedFile": {
                         "latest": {
                             "accessLevel": "public",
                             "contentType": "text/plain",
                             "version": 2,
                             "description": "Plain text file that was updated",
                             "categories": ["test-category-1", "test-category-3"],
                             "tags": ["test-tag1", "test-tag4"],
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
                            "categories": ["test-category-2"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by categories: [test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": null,
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
                        }
                    }
                },
                // {
                //     "path": "/bar.txt",
                //     "ref": file_2_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": "Plain text file",
                //             "categories": ["test-category-2"],
                //             "tags": ["test-tag1", "test-tag2"],
                //         }
                //     }
                // },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by categories: [test-category-2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-2"],
                    "byAccessLevels": null,
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                // {
                //     "path": "/2025/foo.txt",
                //     "ref": file_1_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 2,
                //             "description": "Plain text file that was updated",
                //             "categories": ["test-category-1", "test-category-3"],
                //             "tags": ["test-tag1", "test-tag4"],
                //         }
                //     }
                // },
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
                            "categories": ["test-category-2"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by categories: [test-category-3, test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-3", "test-category-1"],
                    "byAccessLevels": null,
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
                        }
                    }
                },
                // {
                //     "path": "/bar.txt",
                //     "ref": file_2_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": "Plain text file",
                //             "categories": ["test-category-2"],
                //             "tags": ["test-tag1", "test-tag2"],
                //         }
                //     }
                // },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by access levels: [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["public"],
                },
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
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
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
                            "categories": ["test-category-2"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    // Filters by access levels: [holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["holders"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                // {
                //     "path": "/2025/foo.txt",
                //     "ref": file_1_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 2,
                //             "description": "Plain text file that was updated",
                //             "categories": ["test-category-1", "test-category-3"],
                //             "tags": ["test-tag1", "test-tag4"],
                //         }
                //     }
                // },
                // {
                //     "path": "/bar.txt",
                //     "ref": file_2_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": "Plain text file",
                //             "categories": ["test-category-2"],
                //             "tags": ["test-tag1", "test-tag2"],
                //         }
                //     }
                // },
                {
                    "path": "/baz.txt",
                    "ref": file_3_did,
                    "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "holders",
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

    // Filters by access levels: [holders, public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["holders", "public"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 3,
            "nodes": all_entries_nodes,
        })
    );

    // Filters combination: [test-tag4] AND [test-category-1] AND
    // [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "filters": {
                    "byTags": ["test-tag4"],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": ["public"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap()["molecule"]["v2"]["project"]["dataRoom"]["latest"]["entries"],
        json!({
            "totalCount": 1,
            "nodes": [
                {
                    "path": "/2025/foo.txt",
                    "ref": file_1_did,
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
                        }
                    }
                },
                // {
                //     "path": "/bar.txt",
                //     "ref": file_2_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "public",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": "Plain text file",
                //             "categories": ["test-category-2"],
                //             "tags": ["test-tag1", "test-tag2"],
                //         }
                //     }
                // },
                // {
                //     "path": "/baz.txt",
                //     "ref": file_3_did,
                //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                //     "asVersionedFile": {
                //         "latest": {
                //             "accessLevel": "holders",
                //             "contentType": "text/plain",
                //             "version": 1,
                //             "description": null,
                //             "categories": [],
                //             "tags": [],
                //         }
                //     }
                // },
            ],
        })
    );

    /////////////////
    // removeEntry //
    /////////////////

    // Non-existent file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/non-existent-path.txt",
                "changeBy": USER_1,
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
                                "message": "Data room entry not found by path",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual file removal
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "path": "/baz.txt",
                "changeBy": USER_1,
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

    harness.synchronize_agents().await;

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
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
                    "changeBy": USER_1,
                    "asVersionedFile": {
                        "latest": {
                            "accessLevel": "public",
                            "contentType": "text/plain",
                            "version": 2,
                            "description": "Plain text file that was updated",
                            "categories": ["test-category-1", "test-category-3"],
                            "tags": ["test-tag1", "test-tag4"],
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
                            "categories": ["test-category-2"],
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
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    let dataset_registry = harness
        .catalog_authorized
        .get_one::<dyn DatasetRegistry>()
        .unwrap();
    let file_1_dataset_id = odf::DatasetID::from_did_str(file_1_did).unwrap();
    let file_1_dataset = dataset_registry
        .get_dataset_by_ref(&file_1_dataset_id.as_local_ref())
        .await
        .unwrap();
    let file_1_head_before_update = file_1_dataset
        .as_metadata_chain()
        .try_get_ref(&odf::BlockRef::Head)
        .await
        .unwrap()
        .unwrap();
    let cas_expected_head = file_1_head_before_update;

    // Non-existent file
    let random_dataset_id = odf::DatasetID::new_generated_ed25519().1;

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_METADATA_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": random_dataset_id.to_string(),
                "expectedHead": null,
                "accessLevel": "holder",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                "description": "Plain text file that was updated... again",
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag1", "test-tag2", "test-tag3"],
                "contentText": "bye bye bye",
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
                                "message": "Data room entry not found by ref",
                                "__typename": "MoleculeDataRoomUpdateEntryByRefNotFound",
                            }
                        }
                    }
                }
            }
        })
    );

    // Actual update
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_METADATA_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": file_1_did,
                "expectedHead": null,
                "accessLevel": "holders",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                "description": "Plain text file that was updated... again",
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag1", "test-tag2", "test-tag3"],
                "contentText": "bye bye bye",
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
                                "__typename": "MoleculeDataRoomUpdateFileMetadataResultSuccess",
                            }
                        }
                    }
                }
            }
        })
    );
    // CAS failure when expected head mismatches
    let cas_failure_res = GraphQLQueryRequest::new(
        UPDATE_METADATA_QUERY,
        async_graphql::Variables::from_json(json!({
            "ipnftUid": ipnft_uid,
            "ref": file_1_did,
            "expectedHead": cas_expected_head.to_string(),
            "accessLevel": "holders",
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
    .data
    .into_json()
    .unwrap();

    let cas_failure_payload =
        &cas_failure_res["molecule"]["v2"]["project"]["dataRoom"]["updateFileMetadata"];
    pretty_assertions::assert_eq!(
        cas_failure_payload["__typename"],
        "UpdateVersionErrorCasFailed"
    );
    pretty_assertions::assert_eq!(
        cas_failure_payload["message"],
        "Expected head didn't match, dataset was likely updated concurrently"
    );
    pretty_assertions::assert_eq!(
        cas_failure_payload["expectedHead"],
        cas_expected_head.to_string()
    );
    assert!(
        cas_failure_payload["actualHead"].is_string()
            && cas_failure_payload["actualHead"] != cas_failure_payload["expectedHead"]
    );

    harness.synchronize_agents().await;

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ENTRIES_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "filters": null,
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
                            "accessLevel": "holders",
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
                            "categories": ["test-category-2"],
                            "tags": ["test-tag1", "test-tag2"],
                        }
                    }
                },
            ],
        })
    );
    pretty_assertions::assert_eq!(
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
            "encryption_metadata": "{\"version\":0,\"dataToEncryptHash\":\"EM1\",\"accessControlConditions\":\"EM2\",\"encryptedBy\":\"EM3\",\"encryptedAt\":\"EM4\",\"chain\":\"EM5\",\"litSdkVersion\":\"EM6\",\"litNetwork\":\"EM7\",\"templateName\":\"EM8\",\"contractVersion\":\"EM9\"}",
            "molecule_access_level": "holders",
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
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileRemovedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/2025/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/baz.txt",
                        "ref": file_3_did,
                        "accessLevel": "holders",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": file_2_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": file_1_did,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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
    // - Get entries with prefix and maxDepth
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_data_room_as_of_block_hash(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_901";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let create_res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "vitafast",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "901",
            })),
        ))
        .await;
    assert!(create_res.is_ok(), "{create_res:#?}");

    let data_room_did = create_res.data.into_json().unwrap()["molecule"]["v2"]["createProject"]
        ["project"]["dataRoom"]["dataset"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let dataset_registry = harness
        .catalog_authorized
        .get_one::<dyn DatasetRegistry>()
        .unwrap();
    let data_room_dataset = dataset_registry
        .get_dataset_by_id(&odf::DatasetID::from_did_str(&data_room_did).unwrap())
        .await
        .unwrap();

    let upload_1_res = GraphQLQueryRequest::new(
        CREATE_VERSIONED_FILE,
        async_graphql::Variables::from_value(value!({
            "ipnftUid": ipnft_uid,
            "path": "/foo.txt",
            "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello"),
            "contentType": "text/plain",
            "changeBy": USER_1,
            "accessLevel": "public",
            "description": "Plain text file",
            "categories": ["test-category-1"],
            "tags": ["tag-old"],
        })),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    let upload_1_data = upload_1_res.data.into_json().unwrap();
    pretty_assertions::assert_eq!(
        upload_1_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );
    let file_1_did = upload_1_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["entry"]
        ["ref"]
        .as_str()
        .unwrap();

    let data_room_head_v1 = data_room_dataset
        .as_metadata_chain()
        .try_get_ref(&odf::BlockRef::Head)
        .await
        .unwrap()
        .unwrap();
    let file_1_dataset = dataset_registry
        .get_dataset_by_id(&odf::DatasetID::from_did_str(file_1_did).unwrap())
        .await
        .unwrap();
    let file_1_head_v1 = file_1_dataset
        .as_metadata_chain()
        .try_get_ref(&odf::BlockRef::Head)
        .await
        .unwrap()
        .unwrap();

    const UPDATE_METADATA_QUERY: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $ref: DatasetID!, $changeBy: String!, $accessLevel: String!, $description: String, $categories: [String!], $tags: [String!], $contentText: String) {
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

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPDATE_METADATA_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "ref": file_1_did,
                "accessLevel": "public",
                "changeBy": USER_1,
                "description": "Plain text file updated",
                "categories": ["test-category-1"],
                "tags": ["tag-new"],
                "contentText": "hello",
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
                                "__typename": "MoleculeDataRoomUpdateFileMetadataResultSuccess",
                            }
                        }
                    }
                }
            }
        })
    );

    harness.synchronize_agents().await;

    const PROJECT_DATA_ROOM_LATEST_QUERY: &str = indoc!(
        r#"
        query ($ipnftUid: String!) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  latest {
                    entries {
                      nodes {
                        path
                        asVersionedFile {
                          latest {
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
    );

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            PROJECT_DATA_ROOM_LATEST_QUERY,
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
                    "project": {
                        "dataRoom": {
                            "latest": {
                                "entries": {
                                    "nodes": [{
                                        "path": "/foo.txt",
                                        "asVersionedFile": {
                                            "latest": {
                                                "tags": ["tag-new"],
                                            }
                                        }
                                    }]
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let upload_2_res = GraphQLQueryRequest::new(
        CREATE_VERSIONED_FILE,
        async_graphql::Variables::from_value(value!({
            "ipnftUid": ipnft_uid,
            "path": "/bar.txt",
            "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello bar"),
            "contentType": "text/plain",
            "changeBy": USER_1,
            "accessLevel": "public",
        })),
    )
    .execute(&harness.schema, &harness.catalog_authorized)
    .await;
    let upload_2_data = upload_2_res.data.into_json().unwrap();
    pretty_assertions::assert_eq!(
        upload_2_data["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"]["isSuccess"],
        json!(true),
    );

    harness.synchronize_agents().await;

    const PROJECTS_DATA_ROOM_AS_OF_QUERY: &str = indoc!(
        r#"
        query ($dataRoomBlockHash: Multihash!, $fileBlockHash: Multihash!) {
          molecule {
            v2 {
              projects(page: 0, perPage: 100) {
                nodes {
                  dataRoom {
                    asOf(blockHash: $dataRoomBlockHash) {
                      entries {
                        totalCount
                        nodes {
                          path
                          asVersionedFile {
                            asOf(blockHash: $fileBlockHash) {
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
        }
        "#
    );

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            PROJECTS_DATA_ROOM_AS_OF_QUERY,
            async_graphql::Variables::from_json(json!({
                "dataRoomBlockHash": data_room_head_v1.to_string(),
                "fileBlockHash": file_1_head_v1.to_string(),
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "projects": {
                        "nodes": [{
                            "dataRoom": {
                                "asOf": {
                                    "entries": {
                                        "totalCount": 1,
                                        "nodes": [{
                                            "path": "/foo.txt",
                                            "asVersionedFile": {
                                                "asOf": {
                                                    "tags": ["tag-old"],
                                                }
                                            }
                                        }]
                                    }
                                }
                            }
                        }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_announcements_quota_exceeded(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let ipnft_uid = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_902";

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_account_opts(PredefinedAccountOpts {
            is_admin: true,
            ..Default::default()
        })
        .build()
        .await;

    // Create project
    let create_project_res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_PROJECT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftSymbol": "quota-announce",
                "ipnftUid": ipnft_uid,
                "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1",
                "ipnftTokenId": "902",
            })),
        ))
        .await;
    assert!(create_project_res.is_ok(), "{create_project_res:#?}");

    // Set very small quota
    let set_quota_res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc!(
            r#"
            mutation {
              accounts {
                me {
                  quotas {
                    setAccountQuotas(quotas: { storage: { limitTotalBytes: 1 } }) {
                      isSuccess
                    }
                  }
                }
              }
            }
            "#
        )))
        .await;
    assert!(set_quota_res.is_ok(), "{set_quota_res:#?}");

    // Attempt to create announcement - expect quota error surfaced
    let announcement_res = harness
        .execute_authorized_query(async_graphql::Request::new(CREATE_ANNOUNCEMENT).variables(
            async_graphql::Variables::from_json(json!({
                "ipnftUid": ipnft_uid,
                "headline": "Quota limited",
                "body": "This should fail",
                "attachments": [],
                "accessLevel": "public",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                "categories": [],
                "tags": [],
            })),
        ))
        .await;

    let payload = announcement_res.data.into_json().unwrap()["molecule"]["v2"]["project"]
        ["announcements"]["create"]
        .clone();
    pretty_assertions::assert_eq!(payload["isSuccess"], json!(false));
    let msg = payload["message"].as_str().unwrap();
    assert!(msg.contains("Quota exceeded"), "unexpected message: {msg}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_announcements_operations(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    // Create the first project
    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";

    pretty_assertions::assert_eq!(
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
        query ($ipnftUid: String!, $filters: MoleculeAnnouncementsFilters) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                announcements {
                  tail(filters: $filters) {
                    totalCount
                    nodes {
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

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
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

        pretty_assertions::assert_eq!(
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

        pretty_assertions::assert_eq!(
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
    let project_1_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [],
                "accessLevel": "public",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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

        pretty_assertions::assert_eq!(
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
                "accessLevel": "holders",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
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

        pretty_assertions::assert_eq!(
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
                "accessLevel": "holders",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
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

        pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
                "accessLevel": "holders",
                "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
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

    harness.synchronize_agents().await;

    // Announcements are listed as expected
    let all_announcements_tail_nodes = value!([
        {
            "id": project_1_announcement_3_id,
            "headline": "Test announcement 3",
            "body": "Blah blah 3",
            "attachments": [
                {
                    "path": "/foo.txt",
                    "ref": project_1_file_1_dataset_id
                },
                {
                    "path": "/bar.txt",
                    "ref": project_1_file_2_dataset_id
                },
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
                {
                    "path": "/foo.txt",
                    "ref": project_1_file_1_dataset_id
                },
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
            "accessLevel": "public",
            "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
            "categories": ["test-category-1", "test-category-2"],
            "tags": ["test-tag1", "test-tag2", "test-tag3"],
        },
    ]);
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
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
                    "project": {
                        "announcements": {
                            "tail": {
                                "totalCount": 3,
                                "nodes": all_announcements_tail_nodes
                            },
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
                            {
                                "path": "/foo.txt",
                                "ref": project_1_file_1_dataset_id
                            },
                            {
                                "path": "/bar.txt",
                                "ref": project_1_file_2_dataset_id
                            },
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
                            {
                                "path": "/foo.txt",
                                "ref": project_1_file_1_dataset_id
                            },
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
                        "accessLevel": "public",
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
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                    }
                },
            ]
        }
    });

    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    /////////////
    // Filters //
    /////////////

    // Filters without values
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 3,
                                "nodes": all_announcements_tail_nodes
                            },
                        }
                    }
                }
            }
        })
    );

    // Filters by tags: [test-tag2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag2"],
                    "byCategories": null,
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 2,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    {
                                        "id": project_1_announcement_2_id,
                                        "headline": "Test announcement 2",
                                        "body": "Blah blah 2",
                                        "attachments": [
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
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
                                        "accessLevel": "public",
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

    // Filters by tags: [test-tag3]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag3"],
                    "byCategories": null,
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 1,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    // {
                                    //     "id": project_1_announcement_2_id,
                                    //     "headline": "Test announcement 2",
                                    //     "body": "Blah blah 2",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                    //     "categories": ["test-category-1"],
                                    //     "tags": ["test-tag1", "test-tag2"],
                                    // },
                                    {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [],
                                        "accessLevel": "public",
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

    // Filters by tags: [test-tag2, test-tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag2", "test-tag1"],
                    "byCategories": null,
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 3,
                                "nodes": [
                                    {
                                         "id": project_1_announcement_3_id,
                                         "headline": "Test announcement 3",
                                         "body": "Blah blah 3",
                                         "attachments": [
                                             {
                                                 "path": "/foo.txt",
                                                 "ref": project_1_file_1_dataset_id
                                             },
                                             {
                                                 "path": "/bar.txt",
                                                 "ref": project_1_file_2_dataset_id
                                             },
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
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
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
                                        "accessLevel": "public",
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

    // Filters by categories: [test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 2,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    {
                                        "id": project_1_announcement_2_id,
                                        "headline": "Test announcement 2",
                                        "body": "Blah blah 2",
                                        "attachments": [
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
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
                                        "accessLevel": "public",
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

    // Filters by categories: [test-category-2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-2"],
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 1,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    // {
                                    //     "id": project_1_announcement_2_id,
                                    //     "headline": "Test announcement 2",
                                    //     "body": "Blah blah 2",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                    //     "categories": ["test-category-1"],
                                    //     "tags": ["test-tag1", "test-tag2"],
                                    // },
                                    {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [],
                                        "accessLevel": "public",
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

    // Filters by categories: [test-category-2, test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": ["test-category-2", "test-category-1"],
                    "byAccessLevels": null,
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
                        "announcements": {
                            "tail": {
                                "totalCount": 2,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    {
                                         "id": project_1_announcement_2_id,
                                         "headline": "Test announcement 2",
                                         "body": "Blah blah 2",
                                         "attachments": [
                                             {
                                                 "path": "/foo.txt",
                                                 "ref": project_1_file_1_dataset_id
                                             },
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
                                        "accessLevel": "public",
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

    // Filters by access levels: [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["public"],
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
                        "announcements": {
                            "tail": {
                                "totalCount": 1,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    // {
                                    //     "id": project_1_announcement_2_id,
                                    //     "headline": "Test announcement 2",
                                    //     "body": "Blah blah 2",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                    //     "categories": ["test-category-1"],
                                    //     "tags": ["test-tag1", "test-tag2"],
                                    // },
                                    {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [],
                                        "accessLevel": "public",
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

    // Filters by access levels: [holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["holders"],
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
                        "announcements": {
                            "tail": {
                                "totalCount": 2,
                                "nodes": [
                                    {
                                        "id": project_1_announcement_3_id,
                                        "headline": "Test announcement 3",
                                        "body": "Blah blah 3",
                                        "attachments": [
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
                                            {
                                                "path": "/bar.txt",
                                                "ref": project_1_file_2_dataset_id
                                            },
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
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
                                        ],
                                        "accessLevel": "holders",
                                        "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    },
                                    // {
                                    //     "id": project_1_announcement_1_id,
                                    //     "headline": "Test announcement 1",
                                    //     "body": "Blah blah 1",
                                    //     "attachments": [],
                                    //     "accessLevel": "public",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC",
                                    //     "categories": ["test-category-1", "test-category-2"],
                                    //     "tags": ["test-tag1", "test-tag2", "test-tag3"],
                                    // },
                                ]
                            }
                        }
                    }
                }
            }
        })
    );

    // Filters by access levels: [public, holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": ["holders", "public"],
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
                        "announcements": {
                            "tail": {
                                "totalCount": 3,
                                "nodes": [
                                    {
                                        "id": project_1_announcement_3_id,
                                        "headline": "Test announcement 3",
                                        "body": "Blah blah 3",
                                        "attachments": [
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
                                            {
                                                "path": "/bar.txt",
                                                "ref": project_1_file_2_dataset_id
                                            },
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
                                            {
                                                "path": "/foo.txt",
                                                "ref": project_1_file_1_dataset_id
                                            },
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
                                        "accessLevel": "public",
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

    // Filters combination: [test-tag1, test-tag2] AND [test-category-1] AND
    // [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_ANNOUNCEMENTS,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag1", "test-tag2"],
                    "byCategories": ["test-category-2"],
                    "byAccessLevels": ["public"],
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
                        "announcements": {
                            "tail": {
                                "totalCount": 1,
                                "nodes": [
                                    // {
                                    //     "id": project_1_announcement_3_id,
                                    //     "headline": "Test announcement 3",
                                    //     "body": "Blah blah 3",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //         {
                                    //             "path": "/bar.txt",
                                    //             "ref": project_1_file_2_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BE",
                                    //     "categories": [],
                                    //     "tags": ["test-tag1"],
                                    // },
                                    // {
                                    //     "id": project_1_announcement_2_id,
                                    //     "headline": "Test announcement 2",
                                    //     "body": "Blah blah 2",
                                    //     "attachments": [
                                    //         {
                                    //             "path": "/foo.txt",
                                    //             "ref": project_1_file_1_dataset_id
                                    //         },
                                    //     ],
                                    //     "accessLevel": "holders",
                                    //     "changeBy": "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD",
                                    //     "categories": ["test-category-1"],
                                    //     "tags": ["test-tag1", "test-tag2"],
                                    // },
                                    {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [],
                                        "accessLevel": "public",
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_activity(search_variant: GraphQLMoleculeV2HarnessSearchVariant) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";
    const PROJECT_2_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2_10";
    const USER_1: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC";
    const USER_2: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD";

    pretty_assertions::assert_eq!(
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

    harness.synchronize_agents().await;

    // Activities are empty
    let expected_activity_node = value!({
        "activity": {
            "nodes": []
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Create a few versioned files
    let project_1_file_1_dataset_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello foo"),
                "contentType": "text/plain",
                "changeBy": USER_1,
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

        pretty_assertions::assert_eq!(
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
                "changeBy": USER_2,
                "accessLevel": "holders",
                "description": "Plain text file (bar)",
                "categories": ["test-category-2"],
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

        pretty_assertions::assert_eq!(
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Upload new file versions
    const UPLOAD_NEW_VERSION: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $ref: DatasetID!, $content: Base64Usnp!, $changeBy: String!, $accessLevel: String!, $categories: [String!], $tags: [String!]) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  uploadFile(
                    ref: $ref
                    content: $content
                    changeBy: $changeBy
                    accessLevel: $accessLevel
                    categories: $categories
                    tags: $tags
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

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_NEW_VERSION,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "ref": project_1_file_1_dataset_id,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye foo"),
                "changeBy": USER_1,
                "accessLevel": "public",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
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
                            "uploadFile": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_NEW_VERSION,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "ref": project_1_file_2_dataset_id,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye bar"),
                "changeBy": USER_2,
                "accessLevel": "holders",
                "categories": ["test-category-2"],
                "tags": [],
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
                            "uploadFile": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Move a file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "fromPath": "/foo.txt",
                "toPath": "/foo_renamed.txt",
                "changeBy": USER_1,
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Create an announcement for the first project
    let project_1_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [project_1_file_1_dataset_id, project_1_file_2_dataset_id],
                "accessLevel": "public",
                "changeBy": USER_1,
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

        pretty_assertions::assert_eq!(
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

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_1_id,
                        "headline": "Test announcement 1",
                        "body": "Blah blah 1",
                        "attachments": [
                            {
                                "path": "/foo_renamed.txt",
                                "ref": project_1_file_1_dataset_id,
                            },
                            {
                                "path": "/bar.txt",
                                "ref": project_1_file_2_dataset_id,
                            },
                        ],
                        "accessLevel": "public",
                        "changeBy": USER_1,
                        "categories": ["test-category-1"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Upload a new file version
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_NEW_VERSION,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "ref": project_1_file_1_dataset_id,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye foo [2]"),
                "changeBy": USER_1,
                "accessLevel": "public",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
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
                            "uploadFile": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );

    harness.synchronize_agents().await;

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_1_id,
                        "headline": "Test announcement 1",
                        "body": "Blah blah 1",
                        "attachments": [
                            {
                                "path": "/foo_renamed.txt",
                                "ref": project_1_file_1_dataset_id,
                            },
                            {
                                "path": "/bar.txt",
                                "ref": project_1_file_2_dataset_id,
                            },
                        ],
                        "accessLevel": "public",
                        "changeBy": USER_1,
                        "categories": ["test-category-1"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    // Remove a file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/bar.txt",
                "changeBy": USER_2,
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

    harness.synchronize_agents().await;

    // Check project activity events

    let expected_activity_node = value!({
        "activity": {
            "nodes": [
                {
                    "__typename": "MoleculeActivityFileRemovedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityAnnouncementV2",
                    "announcement": {
                        "id": project_1_announcement_1_id,
                        "headline": "Test announcement 1",
                        "body": "Blah blah 1",
                        "attachments": [
                            {
                                "path": "/foo_renamed.txt",
                                "ref": project_1_file_1_dataset_id,
                            },
                            // NOTE: We removed this file from the data room
                            // {
                            //     "path": "/bar.txt",
                            //     "ref": project_1_file_2_dataset_id,
                            // },
                        ],
                        "accessLevel": "public",
                        "changeBy": USER_1,
                        "categories": ["test-category-1"],
                        "tags": ["test-tag1", "test-tag2"],
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileUpdatedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/bar.txt",
                        "ref": project_1_file_2_dataset_id,
                        "accessLevel": "holders",
                        "changeBy": USER_2,
                    }
                },
                {
                    "__typename": "MoleculeActivityFileAddedV2",
                    "entry": {
                        "path": "/foo.txt",
                        "ref": project_1_file_1_dataset_id,
                        "accessLevel": "public",
                        "changeBy": USER_1,
                    }
                },
            ]
        }
    });
    pretty_assertions::assert_eq!(
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
                "v2": expected_activity_node
            }
        })
    );
    pretty_assertions::assert_eq!(
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

    ///////////////////////////////////////////////////////////////////////////////

    // Create another project
    pretty_assertions::assert_eq!(
        {
            let res_json = GraphQLQueryRequest::new(
                CREATE_PROJECT,
                async_graphql::Variables::from_value(value!({
                    "ipnftSymbol": "vitaslow",
                    "ipnftUid": PROJECT_2_UID,
                    "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                    "ipnftTokenId": "10",
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

    // Create an announcement for the second project
    let project_2_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_2_UID,
                "headline": "Test announcement 2",
                "body": "Blah blah 2",
                "attachments": [],
                "accessLevel": "holders",
                "changeBy": USER_2,
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag2"],
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

        pretty_assertions::assert_eq!(
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

    harness.synchronize_agents().await;

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_2_UID,
                "filters": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "project": {
                        "activity": {
                            "nodes": [
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_2_announcement_1_id,
                                        "headline": "Test announcement 2",
                                        "body": "Blah blah 2",
                                        "attachments": [],
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                        "categories": ["test-category-1", "test-category-2"],
                                        "tags": ["test-tag2"],
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Check global activity events
    let expected_all_global_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_2_announcement_1_id,
                "headline": "Test announcement 2",
                "body": "Blah blah 2",
                "attachments": [],
                "accessLevel": "holders",
                "changeBy": USER_2,
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag2"],
            }
        },
        {
            "__typename": "MoleculeActivityFileRemovedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_1_announcement_1_id,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [
                    {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                    },
                    // NOTE: We removed this file from the data room
                    // {
                    //     "path": "/bar.txt",
                    //     "ref": project_1_file_2_dataset_id,
                    // },
                ],
                "accessLevel": "public",
                "changeBy": USER_1,
                "categories": ["test-category-1"],
                "tags": ["test-tag1", "test-tag2"],
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
    ]);
    pretty_assertions::assert_eq!(
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
                "v2": {
                    "activity": {
                        "nodes": expected_all_global_activity_nodes
                    }
                }
            }
        })
    );

    let expected_global_file_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityFileRemovedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
    ]);

    let expected_global_announcement_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_2_announcement_1_id,
                "headline": "Test announcement 2",
                "body": "Blah blah 2",
                "attachments": [],
                "accessLevel": "holders",
                "changeBy": USER_2,
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag2"],
            }
        },
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_1_announcement_1_id,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [
                    {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                    },
                    // NOTE: We removed this file from the data room
                    // {
                    //     "path": "/bar.txt",
                    //     "ref": project_1_file_2_dataset_id,
                    // },
                ],
                "accessLevel": "public",
                "changeBy": USER_1,
                "categories": ["test-category-1"],
                "tags": ["test-tag1", "test-tag2"],
            }
        },
    ]);

    // Filters: byKinds -> only FILE
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byKinds": ["FILE"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": expected_global_file_activity_nodes
                    }
                }
            }
        })
    );

    // Filters: byKinds -> only ANNOUNCEMENT
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byKinds": ["ANNOUNCEMENT"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": expected_global_announcement_activity_nodes
                    }
                }
            }
        })
    );

    /////////////////////
    // Project filters //
    /////////////////////

    let expected_all_project_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityFileRemovedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_1_announcement_1_id,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [
                    {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                    },
                    // NOTE: We removed this file from the data room
                    // {
                    //     "path": "/bar.txt",
                    //     "ref": project_1_file_2_dataset_id,
                    // },
                ],
                "accessLevel": "public",
                "changeBy": USER_1,
                "categories": ["test-category-1"],
                "tags": ["test-tag1", "test-tag2"],
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
    ]);

    let expected_project_file_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityFileRemovedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo_renamed.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileUpdatedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/bar.txt",
                "ref": project_1_file_2_dataset_id,
                "accessLevel": "holders",
                "changeBy": USER_2,
            }
        },
        {
            "__typename": "MoleculeActivityFileAddedV2",
            "entry": {
                "path": "/foo.txt",
                "ref": project_1_file_1_dataset_id,
                "accessLevel": "public",
                "changeBy": USER_1,
            }
        },
    ]);

    let expected_project_announcement_activity_nodes = value!([
        {
            "__typename": "MoleculeActivityAnnouncementV2",
            "announcement": {
                "id": project_1_announcement_1_id,
                "headline": "Test announcement 1",
                "body": "Blah blah 1",
                "attachments": [
                    {
                        "path": "/foo_renamed.txt",
                        "ref": project_1_file_1_dataset_id,
                    },
                    // NOTE: We removed this file from the data room
                    // {
                    //     "path": "/bar.txt",
                    //     "ref": project_1_file_2_dataset_id,
                    // },
                ],
                "accessLevel": "public",
                "changeBy": USER_1,
                "categories": ["test-category-1"],
                "tags": ["test-tag1", "test-tag2"],
            }
        },
    ]);
    // Filters without values
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": null,
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
                        "activity": {
                            "nodes": expected_all_project_activity_nodes
                        }
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": expected_all_project_activity_nodes
                        }
                    }
                }
            }
        })
    );

    // Filters: byKinds -> only FILE
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byKinds": ["FILE"],
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
                        "activity": {
                            "nodes": expected_project_file_activity_nodes
                        }
                    }
                }
            }
        })
    );

    // Filters: byKinds -> only ANNOUNCEMENT
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byKinds": ["ANNOUNCEMENT"],
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
                        "activity": {
                            "nodes": expected_project_announcement_activity_nodes
                        }
                    }
                }
            }
        })
    );

    // Filters by tags: [tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag1"],
                    "byCategories": [],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityAnnouncementV2",
                                //     "announcement": {
                                //         "id": project_2_announcement_1_id,
                                //         "headline": "Test announcement 2",
                                //         "body": "Blah blah 2",
                                //         "attachments": [],
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //         "categories": ["test-category-1", "test-category-2"],
                                //         "tags": ["test-tag2"],
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by tags: [tag2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag2"],
                    "byCategories": [],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by tags: [tag2, tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag2", "test-tag1"],
                    "byCategories": [],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "changeBy": USER_1,
                                //     }
                                // },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-2"],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                {
                                    "__typename": "MoleculeActivityFileRemovedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityAnnouncementV2",
                                //     "announcement": {
                                //         "id": project_1_announcement_1_id,
                                //         "headline": "Test announcement 1",
                                //         "body": "Blah blah 1",
                                //         "attachments": [
                                //             {
                                //                 "path": "/foo_renamed.txt",
                                //                 "ref": project_1_file_1_dataset_id,
                                //             },
                                //             // NOTE: We removed this file from the data room
                                //             // {
                                //             //     "path": "/bar.txt",
                                //             //     "ref": project_1_file_2_dataset_id,
                                //             // },
                                //         ],
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //         "categories": ["test-category-1"],
                                //         "tags": ["test-tag1", "test-tag2"],
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-2, test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-2", "test-category-1"],
                    "byAccessLevels": [],
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
                        "activity": {
                            "nodes": [
                                {
                                    "__typename": "MoleculeActivityFileRemovedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                     }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by access levels: [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["public"],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo_renamed.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/foo.txt",
                                        "ref": project_1_file_1_dataset_id,
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                    }
                                },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by access levels: [holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["holders"],
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
                        "activity": {
                            "nodes": [
                                {
                                    "__typename": "MoleculeActivityFileRemovedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityAnnouncementV2",
                                //     "announcement": {
                                //         "id": project_1_announcement_1_id,
                                //         "headline": "Test announcement 1",
                                //         "body": "Blah blah 1",
                                //         "attachments": [
                                //             {
                                //                 "path": "/foo_renamed.txt",
                                //                 "ref": project_1_file_1_dataset_id,
                                //             },
                                //             // NOTE: We removed this file from the data room
                                //             // {
                                //             //     "path": "/bar.txt",
                                //             //     "ref": project_1_file_2_dataset_id,
                                //             // },
                                //         ],
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //         "categories": ["test-category-1"],
                                //         "tags": ["test-tag1", "test-tag2"],
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileUpdatedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityFileAddedV2",
                                    "entry": {
                                        "path": "/bar.txt",
                                        "ref": project_1_file_2_dataset_id,
                                        "accessLevel": "holders",
                                        "changeBy": USER_2,
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                            ]
                        }
                    }
                }
            }
        })
    );

    // Filters by access levels: [public, holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["public", "holders"],
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
                        "activity": {
                            "nodes": expected_all_project_activity_nodes
                        }
                    }
                }
            }
        })
    );

    // Filters combination: [test-tag1] AND [test-category-1] AND [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_PROJECT_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "filters": {
                    "byTags": ["test-tag1"],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": ["public"],
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
                        "activity": {
                            "nodes": [
                                // {
                                //     "__typename": "MoleculeActivityFileRemovedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                {
                                    "__typename": "MoleculeActivityAnnouncementV2",
                                    "announcement": {
                                        "id": project_1_announcement_1_id,
                                        "headline": "Test announcement 1",
                                        "body": "Blah blah 1",
                                        "attachments": [
                                            {
                                                "path": "/foo_renamed.txt",
                                                "ref": project_1_file_1_dataset_id,
                                            },
                                            // NOTE: We removed this file from the data room
                                            // {
                                            //     "path": "/bar.txt",
                                            //     "ref": project_1_file_2_dataset_id,
                                            // },
                                        ],
                                        "accessLevel": "public",
                                        "changeBy": USER_1,
                                        "categories": ["test-category-1"],
                                        "tags": ["test-tag1", "test-tag2"],
                                    }
                                },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo_renamed.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileUpdatedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/bar.txt",
                                //         "ref": project_1_file_2_dataset_id,
                                //         "accessLevel": "holders",
                                //         "changeBy": USER_2,
                                //     }
                                // },
                                // {
                                //     "__typename": "MoleculeActivityFileAddedV2",
                                //     "entry": {
                                //         "path": "/foo.txt",
                                //         "ref": project_1_file_1_dataset_id,
                                //         "accessLevel": "public",
                                //         "changeBy": USER_1,
                                //     }
                                // },
                            ]
                        }
                    }
                }
            }
        })
    );

    ////////////////////
    // Global filters //
    ////////////////////

    // Filters without values
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": null,
                    "byCategories": null,
                    "byAccessLevels": null,
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": expected_all_global_activity_nodes
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": expected_all_global_activity_nodes
                    }
                }
            }
        })
    );

    // Filters by tags: [tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": ["test-tag1"],
                    "byCategories": [],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            // {
                            //     "__typename": "MoleculeActivityAnnouncementV2",
                            //     "announcement": {
                            //         "id": project_2_announcement_1_id,
                            //         "headline": "Test announcement 2",
                            //         "body": "Blah blah 2",
                            //         "attachments": [],
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //         "categories": ["test-category-1", "test-category-2"],
                            //         "tags": ["test-tag2"],
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                        ]
                    }
                }
            }
        })
    );

    // Filters by tags: [tag2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": ["test-tag2"],
                    "byCategories": [],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                        ]
                    }
                }
            }
        })
    );

    // Filters by tags: [tag2, tag1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": ["test-tag2", "test-tag1"],
                    "byCategories": [],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                 "__typename": "MoleculeActivityAnnouncementV2",
                                 "announcement": {
                                     "id": project_2_announcement_1_id,
                                     "headline": "Test announcement 2",
                                     "body": "Blah blah 2",
                                     "attachments": [],
                                     "accessLevel": "holders",
                                     "changeBy": USER_2,
                                     "categories": ["test-category-1", "test-category-2"],
                                     "tags": ["test-tag2"],
                                 }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                        ]
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                        ]
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-2]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-2"],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileRemovedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityAnnouncementV2",
                            //     "announcement": {
                            //         "id": project_1_announcement_1_id,
                            //         "headline": "Test announcement 1",
                            //         "body": "Blah blah 1",
                            //         "attachments": [
                            //             {
                            //                 "path": "/foo_renamed.txt",
                            //                 "ref": project_1_file_1_dataset_id,
                            //             },
                            //             // NOTE: We removed this file from the data room
                            //             // {
                            //             //     "path": "/bar.txt",
                            //             //     "ref": project_1_file_2_dataset_id,
                            //             // },
                            //         ],
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //         "categories": ["test-category-1"],
                            //         "tags": ["test-tag1", "test-tag2"],
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                        ]
                    }
                }
            }
        })
    );

    // Filters by categories: [test-category-2, test-category-1]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": ["test-category-2", "test-category-1"],
                    "byAccessLevels": [],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            {
                                 "__typename": "MoleculeActivityFileRemovedV2",
                                 "entry": {
                                     "path": "/bar.txt",
                                     "ref": project_1_file_2_dataset_id,
                                     "accessLevel": "holders",
                                     "changeBy": USER_2,
                                 }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                 "__typename": "MoleculeActivityFileUpdatedV2",
                                 "entry": {
                                     "path": "/bar.txt",
                                     "ref": project_1_file_2_dataset_id,
                                     "accessLevel": "holders",
                                     "changeBy": USER_2,
                                 }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                 "__typename": "MoleculeActivityFileAddedV2",
                                 "entry": {
                                     "path": "/bar.txt",
                                     "ref": project_1_file_2_dataset_id,
                                     "accessLevel": "holders",
                                     "changeBy": USER_2,
                                 }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                        ]
                    }
                }
            }
        })
    );

    // Filters by access levels: [public]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["public"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            // {
                            //     "__typename": "MoleculeActivityAnnouncementV2",
                            //     "announcement": {
                            //         "id": project_2_announcement_1_id,
                            //         "headline": "Test announcement 2",
                            //         "body": "Blah blah 2",
                            //         "attachments": [],
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //         "categories": ["test-category-1", "test-category-2"],
                            //         "tags": ["test-tag2"],
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_1_announcement_1_id,
                                    "headline": "Test announcement 1",
                                    "body": "Blah blah 1",
                                    "attachments": [
                                        {
                                            "path": "/foo_renamed.txt",
                                            "ref": project_1_file_1_dataset_id,
                                        },
                                        // NOTE: We removed this file from the data room
                                        // {
                                        //     "path": "/bar.txt",
                                        //     "ref": project_1_file_2_dataset_id,
                                        // },
                                    ],
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                    "categories": ["test-category-1"],
                                    "tags": ["test-tag1", "test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo_renamed.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/foo.txt",
                                    "ref": project_1_file_1_dataset_id,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                        ]
                    }
                }
            }
        })
    );

    // Filters by access levels: [holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["holders"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileRemovedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityAnnouncementV2",
                            //     "announcement": {
                            //         "id": project_1_announcement_1_id,
                            //         "headline": "Test announcement 1",
                            //         "body": "Blah blah 1",
                            //         "attachments": [
                            //             {
                            //                 "path": "/foo_renamed.txt",
                            //                 "ref": project_1_file_1_dataset_id,
                            //             },
                            //             // NOTE: We removed this file from the data room
                            //             // {
                            //             //     "path": "/bar.txt",
                            //             //     "ref": project_1_file_2_dataset_id,
                            //             // },
                            //         ],
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //         "categories": ["test-category-1"],
                            //         "tags": ["test-tag1", "test-tag2"],
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileUpdatedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/bar.txt",
                                    "ref": project_1_file_2_dataset_id,
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                        ]
                    }
                }
            }
        })
    );

    // Filters by access levels: [public, holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": [],
                    "byCategories": [],
                    "byAccessLevels": ["public", "holders"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": expected_all_global_activity_nodes
                    }
                }
            }
        })
    );

    // Filters combination: [test-tag2] AND [test-category-1] AND [holders]
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byTags": ["test-tag2"],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": ["holders"],
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityAnnouncementV2",
                                "announcement": {
                                    "id": project_2_announcement_1_id,
                                    "headline": "Test announcement 2",
                                    "body": "Blah blah 2",
                                    "attachments": [],
                                    "accessLevel": "holders",
                                    "changeBy": USER_2,
                                    "categories": ["test-category-1", "test-category-2"],
                                    "tags": ["test-tag2"],
                                }
                            },
                            // {
                            //     "__typename": "MoleculeActivityFileRemovedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityAnnouncementV2",
                            //     "announcement": {
                            //         "id": project_1_announcement_1_id,
                            //         "headline": "Test announcement 1",
                            //         "body": "Blah blah 1",
                            //         "attachments": [
                            //             {
                            //                 "path": "/foo_renamed.txt",
                            //                 "ref": project_1_file_1_dataset_id,
                            //             },
                            //             // NOTE: We removed this file from the data room
                            //             // {
                            //             //     "path": "/bar.txt",
                            //             //     "ref": project_1_file_2_dataset_id,
                            //             // },
                            //         ],
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //         "categories": ["test-category-1"],
                            //         "tags": ["test-tag1", "test-tag2"],
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo_renamed.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileUpdatedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/bar.txt",
                            //         "ref": project_1_file_2_dataset_id,
                            //         "accessLevel": "holders",
                            //         "changeBy": USER_2,
                            //     }
                            // },
                            // {
                            //     "__typename": "MoleculeActivityFileAddedV2",
                            //     "entry": {
                            //         "path": "/foo.txt",
                            //         "ref": project_1_file_1_dataset_id,
                            //         "accessLevel": "public",
                            //         "changeBy": USER_1,
                            //     }
                            // },
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_activity_access_level_rules_filters(
    search_variant: GraphQLMoleculeV2HarnessSearchVariant,
) {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc9_201";
    const PROJECT_2_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc9_202";

    for (uid, addr, token_id) in [
        (
            PROJECT_1_UID,
            "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc9",
            201,
        ),
        (
            PROJECT_2_UID,
            "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc9",
            202,
        ),
    ] {
        let token_id_str = token_id.to_string();
        assert_eq!(
            {
                let res_json = GraphQLQueryRequest::new(
                    CREATE_PROJECT,
                    async_graphql::Variables::from_value(value!({
                        "ipnftSymbol": format!("sym{token_id}"),
                        "ipnftUid": uid,
                        "ipnftAddress": addr,
                        "ipnftTokenId": token_id_str,
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
    }

    async fn create_file(
        harness: &GraphQLMoleculeV2Harness,
        ipnft_uid: &str,
        path: &str,
        access_level: &str,
    ) -> String {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": ipnft_uid,
                "path": path,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"payload"),
                "contentType": "text/plain",
                "changeBy": USER_1,
                "accessLevel": access_level,
                "description": null,
                "categories": null,
                "tags": null,
                "contentText": null,
                "encryptionMetadata": null,
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        let mut upload_file_node =
            res_json["molecule"]["v2"]["project"]["dataRoom"]["uploadFile"].take();

        upload_file_node["entry"]["ref"]
            .take()
            .as_str()
            .unwrap()
            .to_owned()
    }

    let project_1_public = create_file(&harness, PROJECT_1_UID, "/p1-public.txt", "public").await;
    let project_1_holders =
        create_file(&harness, PROJECT_1_UID, "/p1-holders.txt", "holders").await;
    let project_2_private =
        create_file(&harness, PROJECT_2_UID, "/p2-private.txt", "private").await;

    harness.synchronize_agents().await;

    // Global activity filtered by scoped access rules should only include matching
    // entries
    assert_eq!(
        GraphQLQueryRequest::new(
            LIST_GLOBAL_ACTIVITY_QUERY,
            async_graphql::Variables::from_json(json!({
                "filters": {
                    "byAccessLevelRules": [
                        { "ipnftUid": PROJECT_1_UID, "accessLevels": ["holders", "public"] },
                        { "ipnftUid": PROJECT_2_UID, "accessLevels": ["private"] }
                    ]
                },
            })),
        )
        .execute(&harness.schema, &harness.catalog_authorized)
        .await
        .data,
        value!({
            "molecule": {
                "v2": {
                    "activity": {
                        "nodes": [
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/p2-private.txt",
                                    "ref": project_2_private,
                                    "accessLevel": "private",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/p1-holders.txt",
                                    "ref": project_1_holders,
                                    "accessLevel": "holders",
                                    "changeBy": USER_1,
                                }
                            },
                            {
                                "__typename": "MoleculeActivityFileAddedV2",
                                "entry": {
                                    "path": "/p1-public.txt",
                                    "ref": project_1_public,
                                    "accessLevel": "public",
                                    "changeBy": USER_1,
                                }
                            },
                        ]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_molecule_v2_search(search_variant: GraphQLMoleculeV2HarnessSearchVariant) {
    // NOTE: In this test, we allow different orderings in some places where the
    //       sequence of results may vary.
    let elasticsearch_backed = match &search_variant {
        GraphQLMoleculeV2HarnessSearchVariant::SourceBased => false,
        GraphQLMoleculeV2HarnessSearchVariant::ElasticsearchBased(_) => true,
    };

    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(search_variant)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";
    const PROJECT_2_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2_10";
    const USER_1: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC";
    const USER_2: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD";

    pretty_assertions::assert_eq!(
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

    // Create a few versioned files
    let project_1_file_1_dataset_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello foo"),
                "contentType": "text/plain",
                "changeBy": USER_1,
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

        pretty_assertions::assert_eq!(
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
                "changeBy": USER_2,
                "accessLevel": "holders",
                "description": "Plain text file (bar)",
                "categories": ["test-category-2"],
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

        pretty_assertions::assert_eq!(
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

    // Upload new file versions
    const UPLOAD_NEW_VERSION: &str = indoc!(
        r#"
        mutation ($ipnftUid: String!, $ref: DatasetID!, $content: Base64Usnp!, $changeBy: String!, $accessLevel: String!, $description: String!, $categories: [String!], $tags: [String!]) {
          molecule {
            v2 {
              project(ipnftUid: $ipnftUid) {
                dataRoom {
                  uploadFile(
                    ref: $ref
                    content: $content
                    changeBy: $changeBy
                    accessLevel: $accessLevel
                    description: $description
                    categories: $categories
                    tags: $tags
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

    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_NEW_VERSION,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "ref": project_1_file_1_dataset_id,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye foo"),
                "changeBy": USER_1,
                "accessLevel": "public",
                "description": "Plain text file (foo) -- updated",
                "categories": ["test-category"],
                "tags": ["test-tag1", "test-tag2"],
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
                            "uploadFile": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            UPLOAD_NEW_VERSION,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "ref": project_1_file_2_dataset_id,
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye bar"),
                "changeBy": USER_2,
                "accessLevel": "holders",
                "description": "Plain text file (bar) -- updated",
                "categories": ["test-category-2"],
                "tags": [],
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
                            "uploadFile": {
                                "isSuccess": true,
                                "message": "",
                            }
                        }
                    }
                }
            }
        })
    );

    // Move a file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            MOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "fromPath": "/foo.txt",
                "toPath": "/foo_renamed.txt",
                "changeBy": USER_1,
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

    // Create an announcement for the first project
    let project_1_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "headline": "Test announcement 1",
                "body": "Blah blah 1 text",
                "attachments": [project_1_file_1_dataset_id, project_1_file_2_dataset_id],
                "accessLevel": "public",
                "changeBy": USER_1,
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

        pretty_assertions::assert_eq!(
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

    // Remove a file
    pretty_assertions::assert_eq!(
        GraphQLQueryRequest::new(
            REMOVE_ENTRY_QUERY,
            async_graphql::Variables::from_json(json!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/bar.txt",
                "changeBy": USER_2,
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

    ///////////////////////////////////////////////////////////////////////////////

    // Create another project
    pretty_assertions::assert_eq!(
        {
            let res_json = GraphQLQueryRequest::new(
                CREATE_PROJECT,
                async_graphql::Variables::from_value(value!({
                    "ipnftSymbol": "vitaslow",
                    "ipnftUid": PROJECT_2_UID,
                    "ipnftAddress": "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc2",
                    "ipnftTokenId": "10",
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

    // Create an announcement for the second project
    let project_2_announcement_1_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_ANNOUNCEMENT,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_2_UID,
                "headline": "Test announcement 2",
                "body": "Blah blah 2 text",
                "attachments": [],
                "accessLevel": "holders",
                "changeBy": USER_2,
                "categories": ["test-category-1", "test-category-2"],
                "tags": ["test-tag2"],
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

        pretty_assertions::assert_eq!(
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

    // Create a file for the second project
    let project_2_file_1_dataset_id = {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_2_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello baz"),
                "contentType": "text/plain",
                "changeBy": USER_2,
                "accessLevel": "holders",
                "description": "Plain te-x-t test file (baz)",
                "categories": ["test-category-1"],
                "tags": ["test-tag2"],
                "contentText": "hello foo",
                "encryptionMetadata": null,
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

        pretty_assertions::assert_eq!(
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

    // Synchronize outbox and search agents
    harness.synchronize_agents().await;

    ////////////
    // Search //
    ////////////

    let project_1_file_1_dataset_search_hit_node = json!({
        "__typename": "MoleculeSemanticSearchFoundDataRoomEntry",
        "entry": {
            "accessLevel": "public",
            "asDataset": {
                "id": project_1_file_1_dataset_id
            },
            "asVersionedFile": {
                "matching": {
                    "accessLevel": "public",
                    "categories": ["test-category"],
                    "changeBy": USER_1,
                    "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"bye foo"),
                    "contentText": null,
                    "contentType": "application/octet-stream",
                    "description": "Plain text file (foo) -- updated",
                    "encryptionMetadata": null,
                    "tags": ["test-tag1", "test-tag2"],
                    "version": 2
                }
            },
            "changeBy": USER_1,
            "path": "/foo_renamed.txt",
            "project": {
                "ipnftUid": PROJECT_1_UID,
            },
            "ref": project_1_file_1_dataset_id,
        }
    });
    let project_1_announcement_1_search_hit_node = json!({
        "__typename": "MoleculeSemanticSearchFoundAnnouncement",
        "announcement": {
            "accessLevel": "public",
            "attachments": [
                {
                    "path": "/foo_renamed.txt",
                    "ref": project_1_file_1_dataset_id
                },
                // NOTE: We removed this file from the data room
                // {
                //     "path": "/bar.txt",
                //     "ref": project_1_file_2_dataset_id
                // },
            ],
            "body": "Blah blah 1 text",
            "categories": ["test-category-1"],
            "changeBy": USER_1,
            "headline": "Test announcement 1",
            "id": project_1_announcement_1_id,
            "project": {
                "ipnftUid": PROJECT_1_UID,
            },
            "tags": ["test-tag1", "test-tag2"]
        }
    });
    let project_2_announcement_1_search_hit_node = json!({
        "__typename": "MoleculeSemanticSearchFoundAnnouncement",
        "announcement": {
            "accessLevel": "holders",
            "attachments": [],
            "body": "Blah blah 2 text",
            "categories": ["test-category-1", "test-category-2"],
            "changeBy": USER_2,
            "headline": "Test announcement 2",
            "id": project_2_announcement_1_id,
            "project": {
                "ipnftUid": PROJECT_2_UID,
            },
            "tags": ["test-tag2"]
        }
    });
    let project_2_file_1_dataset_search_hit_node = json!({
        "__typename": "MoleculeSemanticSearchFoundDataRoomEntry",
        "entry": {
            "accessLevel": "holders",
            "asDataset": {
                "id": project_2_file_1_dataset_id
            },
            "asVersionedFile": {
                "matching": {
                    "accessLevel": "holders",
                    "categories": ["test-category-1"],
                    "changeBy": USER_2,
                    "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello baz"),
                    "contentText": "hello foo",
                    "contentType": "text/plain",
                    "description": "Plain te-x-t test file (baz)",
                    "encryptionMetadata": null,
                    "tags": ["test-tag2"],
                    "version": 1
                }
            },
            "changeBy": USER_2,
            "path": "/foo.txt",
            "project": {
                "ipnftUid": PROJECT_2_UID,
            },
            "ref": project_2_file_1_dataset_id,
        }
    });

    // Empty prompt
    pretty_assertions::assert_eq!(
        harness.execute_search_query("", None).await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Prompt: "tEXt" (files + announcements (body))
    pretty_assertions::assert_eq!(
        harness.execute_search_query("tEXt", None).await,
        json!({
            // NOTE: In this case, there are different behaviors that we allow:
            "nodes": if elasticsearch_backed {
                [
                    // project_2_file_1_dataset_search_hit_node,
                    &project_1_file_1_dataset_search_hit_node, // earlier, but higher score
                    &project_2_announcement_1_search_hit_node,
                    &project_1_announcement_1_search_hit_node,
                ]
            } else {
                [
                    // project_2_file_1_dataset_search_hit_node,
                    &project_2_announcement_1_search_hit_node,
                    &project_1_announcement_1_search_hit_node,
                    &project_1_file_1_dataset_search_hit_node,
                ]
            },
            "totalCount": 3
        })
    );

    // Prompt: "tESt" (files + announcements (headline))
    pretty_assertions::assert_eq!(
        harness.execute_search_query("tESt", None).await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 3
        })
    );

    // Prompt: "bLaH" (only announcements (body))
    pretty_assertions::assert_eq!(
        harness.execute_search_query("bLaH", None).await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Prompt: "plain" (only files)
    pretty_assertions::assert_eq!(
        harness.execute_search_query("plain", None).await,
        json!({
            // NOTE: In this case, there are different behaviors that we allow:
            "nodes": if elasticsearch_backed {
                [
                    &project_1_file_1_dataset_search_hit_node, // earlier, but higher score
                    &project_2_file_1_dataset_search_hit_node,
                    // project_2_announcement_1_search_hit_node,
                    // project_1_announcement_1_search_hit_node,
                ]
            } else {
                [
                    &project_2_file_1_dataset_search_hit_node,
                    &project_1_file_1_dataset_search_hit_node,
                    // project_2_announcement_1_search_hit_node,
                    // project_1_announcement_1_search_hit_node,
                ]
            },
            "totalCount": 2
        })
    );

    // Filters: byIpnftUids: [PROJECT_1_UID]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byIpnftUids": [PROJECT_1_UID],
                }))
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                // project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byIpnftUids: [PROJECT_2_UID]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byIpnftUids": [PROJECT_2_UID],
                }))
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                // project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byIpnftUids: [PROJECT_2_UID, PROJECT_1_UID]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byIpnftUids": [PROJECT_2_UID, PROJECT_1_UID],
                }))
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Filters: byKind: [FILE]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byKinds": ["FILE"],
                }))
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                // project_2_announcement_1_search_hit_node,
                // project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byKind: ANNOUNCEMENT
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byKinds": ["ANNOUNCEMENT"],
                }))
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byKind: [ANNOUNCEMENT, FILE]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byKinds": ["ANNOUNCEMENT", "FILE"],
                }))
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Filters: byTags: [test-tag1]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byTags": ["test-tag1"],
                })),
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                // project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byTags: [test-tag2]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byTags": ["test-tag2"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Filters: byTags: [test-tag2, test-tag1]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byTags": ["test-tag2", "test-tag1"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Filters: byCategories: [test-category-1]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byCategories": ["test-category-1"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 3
        })
    );

    // Filters: byCategories: [test-category-2]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byCategories": ["test-category-2"],
                })),
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                // project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 1
        })
    );

    // Filters: byCategories: [test-category-2, test-category-1]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byCategories": ["test-category-2", "test-category-1"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 3
        })
    );

    // Filters: byAccessLevels: ["public"]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byAccessLevels": ["public"],
                })),
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                // project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byAccessLevels: ["holders"]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byAccessLevels": ["holders"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                // project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 2
        })
    );

    // Filters: byAccessLevels: ["holders", "public"]
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "",
                Some(serde_json::json!({
                    "byAccessLevels": ["holders", "public"],
                })),
            )
            .await,
        json!({
            "nodes": [
                project_2_file_1_dataset_search_hit_node,
                project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 4
        })
    );

    // Filters combo: "blah blah 1" + [test-category-1] + [test-tag2] + ["public"] +
    //                + "ONLY_ANNOUNCEMENTS"
    pretty_assertions::assert_eq!(
        harness
            .execute_search_query(
                "blah blah 1",
                Some(serde_json::json!({
                    "byTags": ["test-tag2"],
                    "byCategories": ["test-category-1"],
                    "byAccessLevels": ["public"],
                    "byKinds": ["ANNOUNCEMENT"],
                })),
            )
            .await,
        json!({
            "nodes": [
                // project_2_file_1_dataset_search_hit_node,
                // project_2_announcement_1_search_hit_node,
                project_1_announcement_1_search_hit_node,
                // project_1_file_1_dataset_search_hit_node,
            ],
            "totalCount": 1
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn test_competitive_writing_of_global_activities_src_multi_thread() {
    let harness = GraphQLMoleculeV2Harness::builder()
        .search_variant(GraphQLMoleculeV2HarnessSearchVariant::SourceBased)
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    const PROJECT_1_UID: &str = "0xcaD88677CA87a7815728C72D74B4ff4982d54Fc1_9";
    const USER_1: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC";
    const USER_2: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BD";

    pretty_assertions::assert_eq!(
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

    harness.synchronize_agents().await;

    let harness = Arc::new(harness);

    // Create a few versioned files
    let harness_clone = harness.clone();
    let project_1_file_1_dataset_id = tokio::spawn(async move {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/foo.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello foo"),
                "contentType": "text/plain",
                "changeBy": USER_1,
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
        .execute(&harness_clone.schema, &harness_clone.catalog_authorized)
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

        pretty_assertions::assert_eq!(
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
    });

    let harness_clone = harness.clone();
    let project_1_file_2_dataset_id = tokio::spawn(async move {
        let mut res_json = GraphQLQueryRequest::new(
            CREATE_VERSIONED_FILE,
            async_graphql::Variables::from_value(value!({
                "ipnftUid": PROJECT_1_UID,
                "path": "/bar.txt",
                "content": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"hello bar"),
                "contentType": "text/plain",
                "changeBy": USER_2,
                "accessLevel": "holders",
                "description": "Plain text file (bar)",
                "categories": ["test-category-2"],
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
        .execute(&harness_clone.schema, &harness_clone.catalog_authorized)
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

        pretty_assertions::assert_eq!(
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
    });

    let (project_1_file_1_dataset_id, project_1_file_2_dataset_id) =
        tokio::try_join!(project_1_file_1_dataset_id, project_1_file_2_dataset_id).unwrap();

    harness.synchronize_agents().await;

    pretty_assertions::assert_eq!(
        {
            let mut res_json = GraphQLQueryRequest::new(
                LIST_GLOBAL_ACTIVITY_QUERY,
                async_graphql::Variables::from_json(json!({
                    "filters": null,
                })),
            )
            .execute(&harness.schema, &harness.catalog_authorized)
            .await
            .data
            .into_json()
            .unwrap();

            let mut nodes = res_json["molecule"]["v2"]["activity"]["nodes"].take();

            use serde_json::Value;

            if let Value::Array(ref mut nodes_as_array) = nodes {
                nodes_as_array.sort_by(|a, b| {
                    let change_by_a = a["entry"]["changeBy"].as_str().unwrap();
                    let change_by_b = b["entry"]["changeBy"].as_str().unwrap();
                    change_by_b.cmp(change_by_a)
                });
            } else {
                unimplemented!();
            }

            nodes
        },
        json!([
            {
                "__typename": "MoleculeActivityFileAddedV2",
                "entry": {
                    "path": "/bar.txt",
                    "ref": project_1_file_2_dataset_id,
                    "accessLevel": "holders",
                    "changeBy": USER_2,
                }
            },
            {
                "__typename": "MoleculeActivityFileAddedV2",
                "entry": {
                    "path": "/foo.txt",
                    "ref": project_1_file_1_dataset_id,
                    "accessLevel": "public",
                    "changeBy": USER_1,
                }
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum GraphQLMoleculeV2HarnessSearchVariant {
    SourceBased,
    ElasticsearchBased(Arc<ElasticsearchTestContext>),
}

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GraphQLMoleculeV2Harness {
    base_gql_harness: BaseGQLDatasetHarness,
    maybe_es_base_harness: Option<ElasticsearchBaseHarness>,
    schema: kamu_adapter_graphql::Schema,
    catalog_authorized: dill::Catalog,
    outbox_agent: Arc<dyn OutboxAgent>,
    molecule_account_id: odf::AccountID,
}

#[bon]
impl GraphQLMoleculeV2Harness {
    #[builder]
    async fn new(
        search_variant: GraphQLMoleculeV2HarnessSearchVariant,
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
        #[builder(default = PredefinedAccountOpts {
            is_admin: false,
            can_provision_accounts: true,
        })]
        predefined_account_opts: PredefinedAccountOpts,
    ) -> Self {
        let maybe_es_base_harness = match search_variant {
            GraphQLMoleculeV2HarnessSearchVariant::SourceBased => None,
            GraphQLMoleculeV2HarnessSearchVariant::ElasticsearchBased(es_ctx) => Some(
                ElasticsearchBaseHarness::new(es_ctx, SystemTimeSourceProvider::Default),
            ),
        };
        let elasticsearch_backed = maybe_es_base_harness.is_some();

        let base_gql_harness = {
            let b = BaseGQLDatasetHarness::builder()
                .tenancy_config(tenancy_config)
                .outbox_provider(OutboxProvider::Dispatching)
                .maybe_mock_dataset_action_authorizer(mock_dataset_action_authorizer);
            if let Some(es_base_harness) = &maybe_es_base_harness {
                b.base_catalog(es_base_harness.catalog())
                    .system_time_source_provider(SystemTimeSourceProvider::Inherited)
                    .build()
            } else {
                let base_catalog = dill::CatalogBuilder::new()
                    .add::<kamu_search_services::DummySearchService>()
                    .build();

                b.base_catalog(&base_catalog)
                    .system_time_source_provider(SystemTimeSourceProvider::Default)
                    .build()
            }
        };

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
            .add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository>()
            .add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageConsumptionRepository>();

        kamu_molecule_services::register_dependencies(
            &mut base_builder,
            kamu_molecule_services::MoleculeDomainDependenciesOptions::default(),
        );

        base_builder.add_value(kamu_molecule_domain::MoleculeConfig {
            enable_reads_from_projections: elasticsearch_backed,
        });

        // Embedding mocks
        let mut embeddings_chunker = MockEmbeddingsChunker::new();
        embeddings_chunker.expect_chunk().returning(Ok);

        base_builder.add_value(embeddings_chunker);
        base_builder.bind::<dyn EmbeddingsChunker, MockEmbeddingsChunker>();
        base_builder.add::<DummyEmbeddingsEncoder>();
        base_builder.add::<EmbeddingsProviderImpl>();
        base_builder.add::<InMemoryEmbeddingsCacheRepository>();

        // Finalize catalog
        let base_catalog = base_builder.build();

        let molecule_account_id = odf::AccountID::new_generated_ed25519().1;

        let AuthenticationCatalogsResult {
            catalog_no_subject,
            catalog_authorized,
            ..
        } = authentication_catalogs_ext(
            &base_catalog,
            Some(CurrentAccountSubject::Logged(LoggedAccount {
                account_id: molecule_account_id.clone(),
                account_name: "molecule".parse().unwrap(),
            })),
            predefined_account_opts,
        )
        .await;

        // Initialize outbox agent
        let outbox_agent = catalog_authorized.get_one::<dyn OutboxAgent>().unwrap();
        outbox_agent.run_initialization().await.unwrap();

        // Ensure search indexes schemas are properly initialized
        if elasticsearch_backed {
            let indexing_catalog = dill::CatalogBuilder::new_chained(&catalog_no_subject)
                .add_value(KamuBackgroundCatalog::new(
                    catalog_no_subject,
                    CurrentAccountSubject::new_test(),
                ))
                .build();
            ElasticsearchBaseHarness::run_initial_indexing(&indexing_catalog).await;
        }

        Self {
            base_gql_harness,
            maybe_es_base_harness,
            schema: kamu_adapter_graphql::schema_quiet(),
            catalog_authorized,
            outbox_agent: base_catalog.get_one::<dyn OutboxAgent>().unwrap(),
            molecule_account_id,
        }
    }

    async fn create_projects_dataset(&self) -> CreateDatasetResult {
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

    async fn execute_authorized_query(
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

    async fn projects_metadata_chain_len(&self, dataset_alias: &odf::DatasetAlias) -> usize {
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

    async fn execute_search_query(
        &self,
        prompt: &str,
        filters: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let gql_value = GraphQLQueryRequest::new(
            SEARCH_QUERY,
            async_graphql::Variables::from_json(json!({
                "prompt": prompt,
                "filters": filters,
            })),
        )
        .execute(&self.schema, &self.catalog_authorized)
        .await
        .data;

        let json_value = gql_value.into_json().unwrap();
        json_value["molecule"]["v2"]["search"].clone()
    }

    async fn synchronize_agents(&self) {
        self.outbox_agent.run_while_has_tasks().await.unwrap();

        if let Some(es_base_harness) = &self.maybe_es_base_harness {
            es_base_harness.es_ctx().refresh_indices().await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
