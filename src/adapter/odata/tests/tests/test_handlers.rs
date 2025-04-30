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
use database_common::NoOpDatabasePlugin;
use dill::*;
use indoc::indoc;
use kamu::domain::*;
use kamu::testing::MockDatasetActionAuthorizer;
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, PredefinedAccountsConfig};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacDatasetRegistryFacadeImpl,
    RebacServiceImpl,
};
use kamu_datasets::*;
use kamu_datasets_inmem::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceStub};
use url::Url;

use super::test_api_server::TestAPIServer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! await_client_server_flow {
    ($api_server_handle: expr, $client_handle: expr) => {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_service_handler() {
    let harness = TestHarness::new().await;

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
        pretty_assertions::assert_eq!(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_metadata_handler() {
    let harness = TestHarness::new().await;

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
        pretty_assertions::assert_eq!(
            body,
            indoc!(
                r#"
                <?xml version="1.0" encoding="utf-8"?>
                <edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx" Version="1.0">
                <edmx:DataServices xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" m:DataServiceVersion="3.0" m:MaxDataServiceVersion="3.0">
                <Schema Namespace="default" xmlns="http://schemas.microsoft.com/ado/2009/11/edm">
                <EntityType Name="foo.bar">
                <Key><PropertyRef Name="offset"/></Key>
                <Property Name="offset" Type="Edm.Int64" Nullable="false"/>
                <Property Name="op" Type="Edm.Int32" Nullable="false"/>
                <Property Name="system_time" Type="Edm.DateTimeOffset" Nullable="false"/>
                <Property Name="date" Type="Edm.DateTimeOffset" Nullable="true"/>
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_collection_handler() {
    let harness = TestHarness::new().await;

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
        pretty_assertions::assert_eq!(
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
                <d:system_time m:type="Edm.DateTimeOffset">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTimeOffset">2020-01-01T00:00:00.000Z</d:date>
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
                <d:system_time m:type="Edm.DateTimeOffset">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTimeOffset">2020-01-01T00:00:00.000Z</d:date>
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
                <d:op m:type="Edm.Int32">0</d:op><d:system_time m:type="Edm.DateTimeOffset">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTimeOffset">2020-01-01T00:00:00.000Z</d:date>
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_collection_handler_by_name() {
    let harness = TestHarness::new().await;

    harness.create_simple_dataset().await;

    let collection_url = format!(
        "http://{}/odata/foo.bar(2)",
        harness.api_server.local_addr()
    );

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&collection_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::OK);
        assert_eq!(
            res.headers()["content-type"],
            "application/atom+xml;type=feed;charset=utf-8"
        );

        let body = res.text().await.unwrap();
        pretty_assertions::assert_eq!(
            body,
            indoc!(
                r#"
                <?xml version="1.0" encoding="utf-8"?>
                <entry xml:base="http://example.com/odata/"
                 xmlns="http://www.w3.org/2005/Atom"
                 xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
                 xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
                <id>http://example.com/odata/foo.bar(2)</id>
                <category scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme" term="default.foo.bar"/>
                <link rel="edit" title="foo.bar" href="foo.bar(2)"/>
                <title/>
                <updated>2050-01-01T12:00:00.000Z</updated>
                <author><name/></author>
                <content type="application/xml">
                <m:properties>
                <d:offset m:type="Edm.Int64">2</d:offset>
                <d:op m:type="Edm.Int32">0</d:op><d:system_time m:type="Edm.DateTimeOffset">2050-01-01T12:00:00.000Z</d:system_time>
                <d:date m:type="Edm.DateTimeOffset">2020-01-01T00:00:00.000Z</d:date>
                <d:city m:type="Edm.String">C</d:city><d:population m:type="Edm.Int64">3000</d:population>
                </m:properties>
                </content>
                </entry>
                "#
            )
            .replace('\n', "")
        );
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_collection_handler_by_name_not_found() {
    let harness = TestHarness::new().await;

    harness.create_simple_dataset().await;

    let collection_url = format!(
        "http://{}/odata/foo.bar(99999)",
        harness.api_server.local_addr()
    );

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&collection_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::NOT_FOUND);
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_collection_handler_by_private_dataset_name_not_found() {
    let harness = TestHarness::new_with_authorizer(MockDatasetActionAuthorizer::denying()).await;

    harness.create_simple_dataset().await;

    let collection_url = format!(
        "http://{}/odata/foo.bar(2)",
        harness.api_server.local_addr()
    );

    let client = async move {
        let cl = reqwest::Client::new();
        let res = cl.get(&collection_url).send().await.unwrap();
        assert_eq!(res.status(), http::StatusCode::NOT_FOUND);
    };

    await_client_server_flow!(harness.api_server.run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestHarness {
    temp_dir: tempfile::TempDir,
    catalog: Catalog,
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    api_server: TestAPIServer,
}

impl TestHarness {
    async fn new() -> Self {
        Self::new_with_authorizer(kamu_core::auth::AlwaysHappyDatasetActionAuthorizer::new()).await
    }

    async fn new_with_authorizer<TDatasetAuthorizer: auth::DatasetActionAuthorizer + 'static>(
        dataset_action_authorizer: TDatasetAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let run_info_dir = temp_dir.path().join("run");
        let cache_dir = temp_dir.path().join("cache");
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(RunInfoDir::new(run_info_dir))
                .add_builder(
                    messaging_outbox::OutboxImmediateImpl::builder()
                        .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<DidGeneratorDefault>()
                .add_value(CacheDir::new(cache_dir))
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>()
                .add::<DataFormatRegistryImpl>()
                .add_value(CurrentAccountSubject::new_test())
                .add_value(dataset_action_authorizer)
                .bind::<dyn auth::DatasetActionAuthorizer, TDatasetAuthorizer>()
                .add_value(TenancyConfig::SingleTenant)
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<DatasetLfsBuilderDatabaseBackedImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CreateDatasetUseCaseHelper>()
                .add_value(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                ))
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
                .add::<EngineProvisionerNull>()
                .add_value(EngineConfigDatafusionEmbeddedIngest::default())
                .add::<PushIngestExecutorImpl>()
                .add::<PushIngestPlannerImpl>()
                .add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
                .add::<QueryServiceImpl>()
                .add_value(ServerUrlConfig::new_test(None))
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<InMemoryDatasetReferenceRepository>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<InMemoryDatasetKeyBlockRepository>()
                .add_value(PredefinedAccountsConfig::single_tenant())
                .add::<PredefinedAccountsRegistrator>()
                .add::<RebacServiceImpl>()
                .add::<InMemoryRebacRepository>()
                .add_value(DefaultAccountProperties::default())
                .add_value(DefaultDatasetProperties::default())
                .add::<LoginPasswordAuthProvider>()
                .add::<AccountServiceImpl>()
                .add::<RebacDatasetRegistryFacadeImpl>()
                .add::<InMemoryAccountRepository>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            register_message_dispatcher::<DatasetReferenceMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            );

            b.build()
        };

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        let api_server =
            TestAPIServer::new(catalog.clone(), None, None, TenancyConfig::SingleTenant).await;

        Self {
            temp_dir,
            push_ingest_planner: catalog.get_one().unwrap(),
            push_ingest_executor: catalog.get_one().unwrap(),
            catalog,
            api_server,
        }
    }

    async fn create_simple_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let ds = create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name("foo.bar")
                    .kind(odf::DatasetKind::Root)
                    .push_event(
                        MetadataFactory::add_push_source()
                            .read(odf::metadata::ReadStepCsv {
                                header: Some(true),
                                schema: Some(
                                    ["date TIMESTAMP", "city STRING", "population BIGINT"]
                                        .iter()
                                        .map(|s| (*s).to_string())
                                        .collect(),
                                ),
                                ..odf::metadata::ReadStepCsv::default()
                            })
                            .merge(odf::metadata::MergeStrategyAppend {})
                            .build(),
                    )
                    .push_event(odf::metadata::SetVocab {
                        event_time_column: Some("date".to_string()),
                        ..Default::default()
                    })
                    .build(),
                Default::default(),
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

        self.ingest_from_url(&ds, url::Url::from_file_path(&src_path).unwrap())
            .await;

        ds
    }

    async fn ingest_from_url(&self, created: &CreateDatasetResult, url: Url) {
        let target = ResolvedDataset::from_created(created);

        let ingest_plan = self
            .push_ingest_planner
            .plan_ingest(target.clone(), None, PushIngestOpts::default())
            .await
            .unwrap();

        let ingest_result = self
            .push_ingest_executor
            .execute_ingest(target.clone(), ingest_plan, DataSource::Url(url), None)
            .await
            .unwrap();

        if let PushIngestResult::Updated {
            old_head, new_head, ..
        } = &ingest_result
        {
            target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .unwrap();
        }
    }
}
