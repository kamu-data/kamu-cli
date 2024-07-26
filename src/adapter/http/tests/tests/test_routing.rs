// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ::serde::Deserialize;
use axum::extract::{FromRequestParts, Path};
use axum::routing::IntoMakeService;
use axum::Router;
use dill::Component;
use hyper::server::conn::AddrIncoming;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

use crate::harness::await_client_server_flow;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct RepoFixture {
    tmp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    created_dataset: CreateDatasetResult,
}

async fn setup_repo() -> RepoFixture {
    let tmp_dir = tempfile::tempdir().unwrap();
    let datasets_dir = tmp_dir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<DummyOutboxImpl>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
        .add_value(CurrentAccountSubject::new_test())
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<CreateDatasetFromSnapshotUseCaseImpl>()
        .build();

    let create_dataset_from_snapshot = catalog
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();

    let created_dataset = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    RepoFixture {
        tmp_dir,
        catalog,
        created_dataset,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn setup_server<IdExt, Extractor>(
    catalog: dill::Catalog,
    path: &str,
    identity_extractor: IdExt,
) -> axum::Server<AddrIncoming, IntoMakeService<Router>>
where
    IdExt: Fn(Extractor) -> DatasetRef,
    IdExt: Clone + Send + 'static,
    Extractor: FromRequestParts<()> + Send + 'static,
    <Extractor as FromRequestParts<()>>::Rejection: std::fmt::Debug,
{
    let app = axum::Router::new()
        .nest(
            path,
            kamu_adapter_http::smart_transfer_protocol_router()
                .layer(kamu_adapter_http::DatasetResolverLayer::new(
                    identity_extractor,
                    |_| false, /* does not mater for routing tests */
                ))
                .layer(axum::extract::Extension(catalog)),
        )
        .layer(
            tower::ServiceBuilder::new().layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            ),
        );

    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

    axum::Server::bind(&addr).serve(app.into_make_service())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn setup_client(dataset_url: url::Url, head_expected: Multihash) {
    let catalog = dill::CatalogBuilder::new()
        .add::<auth::DummyOdfServerAccessTokenResolver>()
        .build();

    let dataset = DatasetFactoryImpl::new(IpfsGateway::default(), catalog.get_one().unwrap())
        .get_dataset(&dataset_url, false)
        .await
        .unwrap();

    let head_actual = dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(head_expected, head_actual);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_routing_root() {
    let repo = setup_repo().await;

    let dataset_ref = repo.created_dataset.dataset_handle.as_local_ref();
    let server = setup_server(repo.catalog, "/", move |_: axum::extract::OriginalUri| {
        dataset_ref.clone()
    });

    let dataset_url = url::Url::parse(&format!("http://{}/", server.local_addr(),)).unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize)]
struct DatasetByID {
    dataset_id: DatasetID,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_id() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:dataset_id",
        |Path(p): Path<DatasetByID>| p.dataset_id.as_local_ref(),
    );

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        server.local_addr(),
        repo.created_dataset.dataset_handle.id.as_did_str()
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize)]
struct DatasetByName {
    dataset_name: DatasetName,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_name() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:dataset_name",
        |Path(p): Path<DatasetByName>| DatasetAlias::new(None, p.dataset_name).into_local_ref(),
    );

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        server.local_addr(),
        repo.created_dataset.dataset_handle.alias
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_name_case_insensetive() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:dataset_name",
        |Path(p): Path<DatasetByName>| DatasetAlias::new(None, p.dataset_name).into_local_ref(),
    );

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        server.local_addr(),
        repo.created_dataset
            .dataset_handle
            .alias
            .dataset_name
            .to_ascii_uppercase()
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(Deserialize)]
struct DatasetByAccountAndName {
    account_name: AccountName,
    dataset_name: DatasetName,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_account_and_name() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:account_name/:dataset_name",
        |Path(p): Path<DatasetByAccountAndName>| {
            // TODO: Ignoring account name until DatasetRepository supports multi-tenancy
            DatasetAlias::new(None, p.dataset_name).into_local_ref()
        },
    );

    println!("{}", server.local_addr());

    let dataset_url = url::Url::parse(&format!(
        "http://{}/kamu/{}/",
        server.local_addr(),
        repo.created_dataset.dataset_handle.alias
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_routing_err_invalid_identity_format() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:dataset_id",
        |Path(p): Path<DatasetByID>| p.dataset_id.into_local_ref(),
    );

    let dataset_url = format!("http://{}/this-is-no-a-did/refs/head", server.local_addr());

    let client = async move {
        let res = reqwest::get(dataset_url).await.unwrap();
        assert_eq!(res.status(), http::StatusCode::BAD_REQUEST);
    };

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_routing_err_dataset_not_found() {
    let repo = setup_repo().await;

    let server = setup_server(
        repo.catalog,
        "/:dataset_name",
        |Path(p): Path<DatasetByName>| DatasetAlias::new(None, p.dataset_name).as_local_ref(),
    );

    let dataset_url = format!("http://{}/non.existing.dataset/", server.local_addr());

    let client = async move {
        let res = reqwest::get(dataset_url).await.unwrap();
        assert_eq!(res.status(), http::StatusCode::NOT_FOUND);
    };

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
