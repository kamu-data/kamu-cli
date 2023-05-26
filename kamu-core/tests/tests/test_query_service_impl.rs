// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use kamu::domain::*;
use kamu::infra;
use kamu::infra::utils::records_writers::JsonArrayWriter;
use kamu::infra::utils::s3_context::S3Context;
use kamu::testing::{MetadataFactory, MinioServer, ParquetWriterHelper};
use opendatafabric::*;
use reqwest::Url;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct S3 {
    tmp_dir: tempfile::TempDir,
    minio: MinioServer,
    url: Url,
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn run_s3_server() -> S3 {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let tmp_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_dir.path(), access_key, secret_key).await;

    let url = Url::parse(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    S3 {
        tmp_dir,
        minio,
        url,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(catalog: &dill::Catalog, tempdir: &Path) -> DatasetAlias {
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let dataset = dataset_repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap()
        .dataset;

    let tmp_data_path = tempdir.join("data");
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));
    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![0, 1, 2]));
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();
    ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

    dataset
        .commit_add_data(
            None,
            Some(OffsetInterval { start: 0, end: 3 }),
            Some(tmp_data_path),
            None,
            None,
            None,
            CommitOpts::default(),
        )
        .await
        .unwrap();

    dataset_alias
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_local_workspace(tempdir: &Path) -> dill::Catalog {
    let workspace_layout = Arc::new(infra::WorkspaceLayout::create(tempdir).unwrap());
    let dataset_repo = infra::DatasetRepositoryLocalFs::new(workspace_layout.clone());

    dill::CatalogBuilder::new()
        .add_value(dataset_repo)
        .add_value(workspace_layout.as_ref().clone())
        .bind::<dyn DatasetRepository, infra::DatasetRepositoryLocalFs>()
        .add::<infra::QueryServiceImpl>()
        .bind::<dyn QueryService, infra::QueryServiceImpl>()
        .add::<infra::ObjectStoreRegistryImpl>()
        .bind::<dyn ObjectStoreRegistry, infra::ObjectStoreRegistryImpl>()
        .add_value(infra::ObjectStoreBuilderLocalFs::new())
        .bind::<dyn ObjectStoreBuilder, infra::ObjectStoreBuilderLocalFs>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_s3_workspace(s3: &S3) -> dill::Catalog {
    let (endpoint, bucket, key_prefix) = S3Context::split_url(&s3.url);
    let s3_context = S3Context::from_items(endpoint.clone(), bucket, key_prefix).await;
    let dataset_repo = infra::DatasetRepositoryS3::new(s3_context.clone());

    dill::CatalogBuilder::new()
        .add_value(dataset_repo)
        .bind::<dyn DatasetRepository, infra::DatasetRepositoryS3>()
        .add::<infra::QueryServiceImpl>()
        .bind::<dyn QueryService, infra::QueryServiceImpl>()
        .add::<infra::ObjectStoreRegistryImpl>()
        .bind::<dyn ObjectStoreRegistry, infra::ObjectStoreRegistryImpl>()
        .add_value(infra::ObjectStoreBuilderLocalFs::new())
        .bind::<dyn ObjectStoreBuilder, infra::ObjectStoreBuilderLocalFs>()
        .add_value(infra::ObjectStoreBuilderS3::new(
            s3_context.bucket,
            endpoint.unwrap(),
            true,
        ))
        .bind::<dyn ObjectStoreBuilder, infra::ObjectStoreBuilderS3>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema = query_svc.get_schema(&dataset_ref).await.unwrap();
    assert!(schema.is_some());

    use kamu::infra::utils::schema_utils;

    let mut buf = Vec::new();
    schema_utils::write_schema_parquet_json(&mut buf, &schema.unwrap()).unwrap();
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

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_schema_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path()).await;
    test_dataset_schema_common(catalog, &tempdir).await;
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_dataset_schema_s3() {
    let s3 = run_s3_server().await;
    let catalog = create_catalog_with_s3_workspace(&s3).await;
    test_dataset_schema_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let df = query_svc.tail(&dataset_ref, 1).await.unwrap();
    let record_batches = df.collect().await.unwrap();

    let mut buf = Vec::new();
    let mut writer = Box::new(JsonArrayWriter::new(&mut buf));
    writer.write_batches(&record_batches).unwrap();
    writer.finish().unwrap();

    let data_content = String::from_utf8(buf).unwrap();
    let data_json = serde_json::from_str::<serde_json::Value>(data_content.as_str()).unwrap();

    assert_eq!(data_json, serde_json::json!([{"blah":"c","offset":2}]));
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_tail_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path()).await;
    test_dataset_tail_common(catalog, &tempdir).await;
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_dataset_tail_s3() {
    let s3 = run_s3_server().await;
    let catalog = create_catalog_with_s3_workspace(&s3).await;
    test_dataset_tail_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
