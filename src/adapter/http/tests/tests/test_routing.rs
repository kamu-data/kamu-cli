// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ::serde::Deserialize;
use axum::extract::{FromRequestParts, Path};
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use kamu::domain::*;
use kamu_accounts::{CurrentAccountSubject, DidSecretEncryptionConfig, PredefinedAccountsConfig};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_adapter_http::DatasetAuthorizationLayer;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_datasets::*;
use kamu_datasets_inmem::{
    InMemoryDatasetDataBlockRepository,
    InMemoryDatasetDependencyRepository,
    InMemoryDatasetEntryRepository,
    InMemoryDatasetKeyBlockRepository,
    InMemoryDatasetReferenceRepository,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;
use utoipa_axum::router::OpenApiRouter;

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

    let mut b = dill::CatalogBuilder::new();
    b.add::<SystemTimeSourceDefault>()
        .add::<DidGeneratorDefault>()
        .add_builder(messaging_outbox::OutboxImmediateImpl::builder(
            messaging_outbox::ConsumerFilter::AllConsumers,
        ))
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<DatabaseTransactionRunner>()
        .add::<DependencyGraphServiceImpl>()
        .add::<InMemoryDatasetDependencyRepository>()
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
            datasets_dir,
        ))
        .add::<kamu_datasets_services::DatasetLfsBuilderDatabaseBackedImpl>()
        .add_value(kamu_datasets_services::MetadataChainDbBackedConfig::default())
        .add_value(CurrentAccountSubject::new_test())
        .add::<AlwaysHappyDatasetActionAuthorizer>()
        .add::<CreateDatasetFromSnapshotUseCaseImpl>()
        .add::<CreateDatasetUseCaseImpl>()
        .add::<CreateDatasetUseCaseHelper>()
        .add::<DatasetReferenceServiceImpl>()
        .add::<InMemoryDatasetReferenceRepository>()
        .add::<DatasetEntryServiceImpl>()
        .add::<InMemoryDatasetEntryRepository>()
        .add::<InMemoryDatasetKeyBlockRepository>()
        .add::<InMemoryDatasetDataBlockRepository>()
        .add::<AccountServiceImpl>()
        .add::<UpdateAccountUseCaseImpl>()
        .add::<CreateAccountUseCaseImpl>()
        .add::<InMemoryAccountRepository>()
        .add::<PredefinedAccountsRegistrator>()
        .add::<RebacServiceImpl>()
        .add::<InMemoryRebacRepository>()
        .add::<InMemoryDidSecretKeyRepository>()
        .add_value(DidSecretEncryptionConfig::sample())
        .add_value(DefaultAccountProperties::default())
        .add_value(DefaultDatasetProperties::default())
        .add_value(PredefinedAccountsConfig::single_tenant())
        .add::<LoginPasswordAuthProvider>();

    NoOpDatabasePlugin::init_database_components(&mut b);

    register_message_dispatcher::<DatasetLifecycleMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    );

    register_message_dispatcher::<DatasetReferenceMessage>(
        &mut b,
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    );

    let catalog = b.build();

    init_on_startup::run_startup_jobs(&catalog).await.unwrap();

    let create_dataset_from_snapshot = catalog
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();

    let created_dataset = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(odf::DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
            Default::default(),
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

async fn setup_server<IdExt, Extractor>(
    catalog: dill::Catalog,
    path: &str,
    identity_extractor: IdExt,
) -> (
    impl std::future::Future<Output = Result<(), std::io::Error>>,
    SocketAddr,
)
where
    IdExt: Fn(Extractor) -> odf::DatasetRef,
    IdExt: Clone + Send + Sync + 'static,
    Extractor: FromRequestParts<()> + Send + Sync + 'static,
    <Extractor as FromRequestParts<()>>::Rejection: std::fmt::Debug,
{
    let (router, _api) = OpenApiRouter::new()
        .nest(
            path,
            kamu_adapter_http::smart_transfer_protocol_router()
                .layer(DatasetAuthorizationLayer::default())
                .layer(kamu_adapter_http::DatasetResolverLayer::new(
                    identity_extractor,
                    |_| false, /* does not matter for routing tests */
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
        )
        .split_for_parts();

    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let server = axum::serve(listener, router.into_make_service());
    (server.into_future(), local_addr)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn setup_client(dataset_url: url::Url, head_expected: odf::Multihash) {
    let catalog = dill::CatalogBuilder::new()
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .build();

    use odf::dataset::DatasetFactory as _;
    let dataset = DatasetFactoryImpl::new(IpfsGateway::default(), catalog.get_one().unwrap(), None)
        .get_dataset(&dataset_url, false)
        .await
        .unwrap();

    let head_actual = dataset
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(head_expected, head_actual);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_routing_root() {
    let repo = setup_repo().await;

    let dataset_ref = repo.created_dataset.dataset_handle.as_local_ref();

    // Cannot reuse setup_server as we need to use `merge` instead of `nest` for
    // root-level
    let (router, _api) = OpenApiRouter::new()
        .merge(
            kamu_adapter_http::smart_transfer_protocol_router()
                .layer(DatasetAuthorizationLayer::default())
                .layer(kamu_adapter_http::DatasetResolverLayer::new(
                    move |_: axum::extract::OriginalUri| dataset_ref.clone(),
                    |_| false, /* does not matter for routing tests */
                ))
                .layer(axum::extract::Extension(repo.catalog)),
        )
        .layer(
            tower::ServiceBuilder::new().layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            ),
        )
        .split_for_parts();

    let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let server = axum::serve(listener, router.into_make_service()).into_future();

    let dataset_url = url::Url::parse(&format!("http://{local_addr}/")).unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize)]
struct DatasetByID {
    dataset_id: odf::DatasetID,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_id() {
    let repo = setup_repo().await;

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{dataset_id}",
        |Path(p): Path<DatasetByID>| p.dataset_id.as_local_ref(),
    )
    .await;

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        local_addr,
        repo.created_dataset.dataset_handle.id.as_did_str()
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize)]
struct DatasetByName {
    dataset_name: odf::DatasetName,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_name() {
    let repo = setup_repo().await;

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{dataset_name}",
        |Path(p): Path<DatasetByName>| {
            odf::DatasetAlias::new(None, p.dataset_name).into_local_ref()
        },
    )
    .await;

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        local_addr, repo.created_dataset.dataset_handle.alias
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_name_case_insensetive() {
    let repo = setup_repo().await;

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{dataset_name}",
        |Path(p): Path<DatasetByName>| {
            odf::DatasetAlias::new(None, p.dataset_name).into_local_ref()
        },
    )
    .await;

    let dataset_url = url::Url::parse(&format!(
        "http://{}/{}/",
        local_addr,
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
    account_name: odf::AccountName,
    dataset_name: odf::DatasetName,
}

#[test_log::test(tokio::test)]
async fn test_routing_dataset_account_and_name() {
    let repo = setup_repo().await;

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{account_name}/{dataset_name}",
        |Path(p): Path<DatasetByAccountAndName>| {
            // TODO: Ignoring account name until DatasetRepository supports multi-tenancy
            odf::DatasetAlias::new(None, p.dataset_name).into_local_ref()
        },
    )
    .await;

    println!("{local_addr}");

    let dataset_url = url::Url::parse(&format!(
        "http://{}/kamu/{}/",
        local_addr, repo.created_dataset.dataset_handle.alias
    ))
    .unwrap();

    let client = setup_client(dataset_url, repo.created_dataset.head);

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_routing_err_invalid_identity_format() {
    let repo = setup_repo().await;

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{dataset_id}",
        |Path(p): Path<DatasetByID>| p.dataset_id.into_local_ref(),
    )
    .await;

    let dataset_url = format!("http://{local_addr}/this-is-no-a-did/refs/head");

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

    let (server, local_addr) = setup_server(
        repo.catalog,
        "/{dataset_name}",
        |Path(p): Path<DatasetByName>| odf::DatasetAlias::new(None, p.dataset_name).as_local_ref(),
    )
    .await;

    let dataset_url = format!("http://{local_addr}/non.existing.dataset/");

    let client = async move {
        let res = reqwest::get(dataset_url).await.unwrap();
        assert_eq!(res.status(), http::StatusCode::NOT_FOUND);
    };

    await_client_server_flow!(server, client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
