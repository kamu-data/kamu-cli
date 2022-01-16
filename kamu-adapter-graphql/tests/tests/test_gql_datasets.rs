// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

use kamu::domain::*;
use kamu::infra;
use kamu::testing::MetadataFactory;
use opendatafabric::*;

#[tokio::test]
async fn dataset_by_id_does_not_exist() {
    let mut cat = dill::Catalog::new();
    cat.add_value(infra::DatasetRegistryNull);
    cat.bind::<dyn DatasetRegistry, infra::DatasetRegistryNull>()
        .unwrap();
    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema 
        .execute("{ datasets { byId (datasetId: \"did:odf:z4k88e8n8Je6fC9Lz9FHrZ7XGsikEyBwTwtMBzxp4RH9pbWn4UM\") { name } } }")
        .await;
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": null,
            }
        })
    );
}

#[tokio::test]
async fn dataset_by_id() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = infra::WorkspaceLayout::create(tempdir.path()).unwrap();

    let mut cat = dill::Catalog::new();
    cat.add_value(workspace_layout);
    cat.add::<infra::DatasetRegistryImpl>();
    cat.bind::<dyn DatasetRegistry, infra::DatasetRegistryImpl>()
        .unwrap();

    let dataset_reg = cat.get_one::<dyn DatasetRegistry>().unwrap();
    let (dataset_handle, _) = dataset_reg
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .unwrap();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(format!(
            "{{ datasets {{ byId (datasetId: \"{}\") {{ name }} }} }}",
            dataset_handle.id
        ))
        .await;
    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "name": "foo",
                }
            }
        })
    );
}
