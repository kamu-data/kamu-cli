// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::harness::await_client_server_flow;
use ::serde::Deserialize;
use axum::{
    extract::{FromRequestParts, Path},
    http::StatusCode,
    routing::IntoMakeService,
    Router,
};
use hyper::server::conn::AddrIncoming;
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct RepoFixture {
    tmp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    created_dataset: CreateDatasetResult,
}

async fn setup_repo() -> RepoFixture {
    let tmp_dir = tempfile::tempdir().unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add_value(WorkspaceLayout::create(tmp_dir.path()).unwrap())
        .add::<DatasetRepositoryLocalFs>()
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let local_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    let created_dataset = local_repo
        .create_dataset_from_snapshot(
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

/////////////////////////////////////////////////////////////////////////////////////////

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
            kamu_adapter_http::smart_transfer_protocol_routes()
                .layer(kamu_adapter_http::DatasetResolverLayer::new(
                    identity_extractor,
                ))
                .layer(axum::extract::Extension(catalog)),
        )
        .layer(
            tower::ServiceBuilder::new().layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![axum::http::Method::GET, axum::http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            ),
        );

    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

    axum::Server::bind(&addr).serve(app.into_make_service())
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn setup_client(dataset_url: url::Url, head_expected: Multihash) {
    let dataset = DatasetFactoryImpl::new(IpfsGateway::default())
        .get_dataset(&dataset_url, false)
        .await
        .unwrap();

    let head_actual = dataset
        .as_metadata_chain()
        .get_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(head_expected, head_actual);
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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
        repo.created_dataset.dataset_handle.id.to_did_string()
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    };

    await_client_server_flow!(server, client);
}

/////////////////////////////////////////////////////////////////////////////////////////

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
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    };

    await_client_server_flow!(server, client);
}

/////////////////////////////////////////////////////////////////////////////////////////
