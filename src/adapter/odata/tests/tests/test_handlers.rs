// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use dill::*;
use event_bus::EventBus;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use url::Url;

use super::test_api_server::TestAPIServer;

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! await_client_server_flow {
    ($api_server_handle: expr, $client_handle: expr) => {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_service_handler() {
    let harness = TestHarness::new();

    harness.create_simple_dataset().await;

    let service_url = format!("http://{}/odata", harness.api_server.local_addr());

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&service_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.headers()["content-type"],
            "application/xml;charset=utf-8"
        );

        let body = res.text().await.unwrap();
        assert_eq!(
            body,
            indoc!(
                r#"
                <?xml version="1.0" encoding="utf-8"?>
                <service xml:base="http://example.com/odata"
                 xmlns="http://www.w3.org/2007/app"
                 xmlns:atom="http://www.w3.org/2005/Atom">
                <workspace>
                <atom:title>default</atom:title>
                <collection href="foo.bar">
                <atom:title>foo.bar</atom:title>
                </collection>
                </workspace>
                </service>
                "#
            )
            .replace('\n', "")
        );
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_metadata_handler() {
    let harness = TestHarness::new();

    harness.create_simple_dataset().await;

    let service_url = format!("http://{}/odata/$metadata", harness.api_server.local_addr());

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&service_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.headers()["content-type"],
            "application/xml;charset=utf-8"
        );

        let body = res.text().await.unwrap();
        assert_eq!(
            body,
            indoc!(
                r#"
                <?xml version="1.0" encoding="utf-8"?>
                <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
                <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="3.0" m:MaxDataServiceVersion="3.0">
                <Schema Namespace="default" xmlns="http://schemas.microsoft.com/ado/2009/11/edm">
                <EntityType Name="foo.bar">
                <Key><PropertyRef Name="foo.bar"/></Key>
                <Property Name="offset" Type="Edm.Int64" Nullable="true"/>
                <Property Name="op" Type="Edm.Int32" Nullable="false"/>
                <Property Name="system_time" Type="Edm.DateTime" Nullable="false"/>
                <Property Name="date" Type="Edm.DateTime" Nullable="true"/>
                <Property Name="city" Type="Edm.String" Nullable="true"/>
                <Property Name="population" Type="Edm.Int64" Nullable="true"/>
                </EntityType>
                <EntityContainer Name="default" m:IsDefaultEntityContainer="true">
                <EntitySet Name="foo.bar" EntityType="default.foo.bar"/>
                </EntityContainer>
                </Schema>
                </edmx:DataServices>
                </edmx:Edmx>
                "#
            )
            .replace('\n', "")
        );
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_collection_handler() {
    let harness = TestHarness::new();

    harness.create_simple_dataset().await;

    let collection_url = format!("http://{}/odata/foo.bar", harness.api_server.local_addr());

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&collection_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.headers()["content-type"],
            "application/atom+xml;type=feed;charset=utf-8"
        );

        let body = res.text().await.unwrap();
        assert_eq!(
            body,
            indoc!(
                r#"
                <?xml version="1.0" encoding="utf-8"?>
                <feed xml:base="http://example.com/odata/"
                 xmlns="http://www.w3.org/2005/Atom"
                 xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
                 xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
                <id>http://example.com/odata/foo.bar</id>
                <title type="text">foo.bar</title>
                <updated>2050-01-01T12:00:00.000Z</updated>
                <link rel="self" title="foo.bar" href="foo.bar"/>
                <entry>
                <id>http://example.com/odata/foo.bar(0)</id>
                <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.foo.bar"/>
                <link rel="edit" title="foo.bar" href="foo.bar(0)"/>
                <title/>
                <updated>2050-01-01T12:00:00.000Z</updated>
                <author><name/></author>
                <content type="application/xml">
                <m:properties>
                <d:offset m:type="Edm.Int64">0</d:offset>
                <d:op m:type="Edm.Int32">0</d:op>
                <d:system_time m:type="Edm.DateTime">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTime">2020-01-01T00:00:00.000Z</d:date>
                <d:city m:type="Edm.String">A</d:city>
                <d:population m:type="Edm.Int64">1000</d:population>
                </m:properties>
                </content>
                </entry>
                <entry>
                <id>http://example.com/odata/foo.bar(1)</id>
                <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.foo.bar"/>
                <link rel="edit" title="foo.bar" href="foo.bar(1)"/>
                <title/>
                <updated>2050-01-01T12:00:00.000Z</updated>
                <author><name/></author>
                <content type="application/xml">
                <m:properties>
                <d:offset m:type="Edm.Int64">1</d:offset>
                <d:op m:type="Edm.Int32">0</d:op>
                <d:system_time m:type="Edm.DateTime">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTime">2020-01-01T00:00:00.000Z</d:date>
                <d:city m:type="Edm.String">B</d:city>
                <d:population m:type="Edm.Int64">2000</d:population>
                </m:properties>
                </content>
                </entry>
                <entry>
                <id>http://example.com/odata/foo.bar(2)</id>
                <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.foo.bar"/>
                <link rel="edit" title="foo.bar" href="foo.bar(2)"/>
                <title/>
                <updated>2050-01-01T12:00:00.000Z</updated>
                <author><name/></author>
                <content type="application/xml">
                <m:properties>
                <d:offset m:type="Edm.Int64">2</d:offset>
                <d:op m:type="Edm.Int32">0</d:op><d:system_time m:type="Edm.DateTime">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTime">2020-01-01T00:00:00.000Z</d:date>
                <d:city m:type="Edm.String">C</d:city><d:population m:type="Edm.Int64">3000</d:population>
                </m:properties>
                </content>
                </entry>
                </feed>
                "#
            )
            .replace('\n', "")
        );
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestHarness {
    temp_dir: tempfile::TempDir,
    dataset_repo: Arc<dyn DatasetRepository>,
    push_ingest_svc: Arc<dyn PushIngestService>,
    api_server: TestAPIServer,
}

impl TestHarness {
    fn new() -> Self {
        Self::new_with_authorizer(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new())
    }

    fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        dataset_action_authorizer: TDatasetAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(cache_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<EventBus>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(SystemTimeSourceStub::new_set(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<EngineProvisionerNull>()
            .add_builder(
                PushIngestServiceImpl::builder()
                    .with_object_store_registry(Arc::new(ObjectStoreRegistryImpl::new(vec![
                        Arc::new(ObjectStoreBuilderLocalFs::new()),
                    ])))
                    .with_data_format_registry(Arc::new(DataFormatRegistryImpl::new()))
                    .with_run_info_dir(run_info_dir),
            )
            .bind::<dyn PushIngestService, PushIngestServiceImpl>()
            .add::<QueryServiceImpl>()
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_rest: Url::parse("http://example.com").unwrap(),
                base_url_flightsql: Url::parse("grpc://localhost:50050").unwrap(),
            }))
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let push_ingest_svc = catalog.get_one::<dyn PushIngestService>().unwrap();

        let api_server = TestAPIServer::new(catalog.clone(), None, None, false);

        Self {
            temp_dir,
            dataset_repo,
            push_ingest_svc,
            api_server,
        }
    }

    async fn create_simple_dataset(&self) -> CreateDatasetResult {
        let ds = self
            .dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name("foo.bar")
                    .kind(DatasetKind::Root)
                    .push_event(
                        MetadataFactory::add_push_source()
                            .read(ReadStepCsv {
                                header: Some(true),
                                schema: Some(
                                    ["date TIMESTAMP", "city STRING", "population BIGINT"]
                                        .iter()
                                        .map(|s| (*s).to_string())
                                        .collect(),
                                ),
                                ..ReadStepCsv::default()
                            })
                            .merge(MergeStrategyAppend {})
                            .build(),
                    )
                    .push_event(SetVocab {
                        event_time_column: Some("date".to_string()),
                        ..Default::default()
                    })
                    .build(),
            )
            .await
            .unwrap();

        let src_path = self.temp_dir.path().join("data.csv");
        std::fs::write(
            &src_path,
            indoc!(
                "
                date,city,population
                2020-01-01,A,1000
                2020-01-01,B,2000
                2020-01-01,C,3000
                "
            ),
        )
        .unwrap();

        self.push_ingest_svc
            .ingest_from_url(
                &ds.dataset_handle.as_local_ref(),
                None,
                url::Url::from_file_path(&src_path).unwrap(),
                None,
                None,
            )
            .await
            .unwrap();

        ds
    }
}
