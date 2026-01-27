// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::SchemaServiceImpl;
use kamu_core::SchemaService;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use tempfile::TempDir;
use test_utils::LocalS3Server;

use crate::tests::queries::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_parquet_schema(catalog: &dill::Catalog, tempdir: &TempDir) {
    let target = helpers::create_test_dataset(catalog, tempdir.path(), "foo").await;

    let schema_service = catalog.get_one::<dyn SchemaService>().unwrap();
    let schema = schema_service
        .get_last_data_chunk_schema_parquet(target)
        .await
        .unwrap();
    assert!(schema.is_some());

    let mut buf = Vec::new();
    odf::utils::schema::format::write_schema_parquet_json(&mut buf, &schema.unwrap()).unwrap();
    let schema_content = String::from_utf8(buf).unwrap();
    let data_schema_json =
        serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

    assert_eq!(
        data_schema_json,
        serde_json::json!({
            "name": "arrow_schema",
            "type": "struct",
            "fields": [{
                "name": "offset",
                "repetition": "REQUIRED",
                "type": "INT64",
                "logicalType": "INTEGER(64,false)"
            }, {
                "name": "blah",
                "repetition": "REQUIRED",
                "type": "BYTE_ARRAY",
                "logicalType": "STRING"
            }]
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema(catalog: &dill::Catalog, tempdir: &TempDir) {
    let target = helpers::create_test_dataset(catalog, tempdir.path(), "foo").await;

    let schema_service = catalog.get_one::<dyn SchemaService>().unwrap();
    let schema = schema_service.get_schema(target).await.unwrap().unwrap();

    odf::utils::testing::assert_odf_schema_eq(
        &schema,
        &odf::schema::DataSchema::new(vec![
            odf::schema::DataField::u64("offset"),
            odf::schema::DataField::string("blah"),
        ]),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn prepare_schema_test_catalog() -> (TempDir, dill::Catalog) {
    let mut authorizer = MockDatasetActionAuthorizer::new();
    authorizer
        .expect_filter_datasets_allowing()
        .returning(|_, _| Ok(vec![]));

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), authorizer);
    (tempdir, catalog)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn prepare_schema_test_s3_catalog() -> (LocalS3Server, dill::Catalog) {
    let mut authorizer = MockDatasetActionAuthorizer::new();
    authorizer
        .expect_filter_datasets_allowing()
        .returning(|_, _| Ok(vec![]));

    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(&s3, authorizer).await;
    (s3, catalog)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_local_fs() {
    let (tempdir, catalog) = prepare_schema_test_catalog();
    test_dataset_parquet_schema(&catalog, &tempdir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_local_fs() {
    let (tempdir, catalog) = prepare_schema_test_catalog();
    test_dataset_schema(&catalog, &tempdir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_s3() {
    let (s3, catalog) = prepare_schema_test_s3_catalog().await;
    test_dataset_schema(&catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_s3() {
    let (s3, catalog) = prepare_schema_test_s3_catalog().await;
    test_dataset_parquet_schema(&catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog_with_local_workspace(
    tempdir: &std::path::Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_local_catalog =
        helpers::create_base_catalog_with_local_workspace(tempdir, dataset_action_authorizer);

    dill::CatalogBuilder::new_chained(&base_local_catalog)
        .add::<SchemaServiceImpl>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_s3_workspace(
    s3: &LocalS3Server,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_s3_catalog =
        helpers::create_base_catalog_with_s3_workspace(s3, dataset_action_authorizer).await;

    dill::CatalogBuilder::new_chained(&base_s3_catalog)
        .add::<SchemaServiceImpl>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
