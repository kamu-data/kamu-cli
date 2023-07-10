// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::*;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::serde::yaml::YamlMetadataEventSerializer;
use opendatafabric::*;

#[test_log::test(tokio::test)]
async fn metadata_chain_append_event() {
    let tempdir = tempfile::tempdir().unwrap();
    let workspace_layout = Arc::new(WorkspaceLayout::create(tempdir.path(), false).unwrap());
    let dataset_repo = DatasetRepositoryLocalFs::new(
        workspace_layout.datasets_dir.clone(),
        Arc::new(CurrentAccountConfig::new("kamu", false)),
        false,
    );

    let cat = dill::CatalogBuilder::new()
        .add_value(dataset_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let dataset_repo = cat.get_one::<dyn DatasetRepository>().unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            None,
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .build(),
        )
        .await
        .unwrap();

    let event = MetadataFactory::set_polling_source().build();

    let event_yaml = String::from_utf8_lossy(
        &YamlMetadataEventSerializer
            .write_manifest(&MetadataEvent::SetPollingSource(event))
            .unwrap(),
    )
    .to_string();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(
            indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName (accountName: "kamu", datasetName: "foo") {
                            metadata {
                                chain {
                                    commitEvent (
                                        event: "<content>",
                                        eventFormat: YAML,
                                    ) {
                                        ... on CommitResultSuccess {
                                            oldHead
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("<content>", &event_yaml.escape_default().to_string()),
        )
        .await;
    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byOwnerAndName": {
                    "metadata": {
                        "chain": {
                            "commitEvent": {
                                "oldHead": create_result.head.to_string(),
                            }
                        }
                    }
                }
            }
        })
    );
}
