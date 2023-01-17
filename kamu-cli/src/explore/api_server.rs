// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu::domain::{Dataset, LocalDatasetRepository, GetDatasetError, BlockRef};
use opendatafabric::{
    DatasetRefLocal, 
    DatasetName, 
    Multihash, 
    serde::{
        flatbuffers::FlatbuffersMetadataBlockSerializer, 
        MetadataBlockSerializer
    }
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr}, 
    collections::HashMap, 
    sync::Arc, 
    str::FromStr
};

pub struct APIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
}

impl APIServer {

    const PARAMETER_DATASET_NAME: &'static str = "datasetName";
    const PARAMETER_REF: &'static str = "ref";
    const PARAMETER_BLOCK_HASH: &'static str = "blockHash";
    const PARAMETER_PHYSICAL_HASH: &'static str = "physicalHash";


    pub fn new(catalog: Catalog, address: Option<IpAddr>, port: Option<u16>) -> Self {
        let local_repo: Arc<dyn LocalDatasetRepository> = catalog.get_one().unwrap();
        let gql_schema = kamu_adapter_graphql::schema(catalog);

        let dataset_routes = axum::Router::new()
            .route(
                format!("/refs/:{}", APIServer::PARAMETER_REF).as_str(), 
                axum::routing::get(dataset_refs_handler)
            )
            .route(
                format!("/blocks/:{}", APIServer::PARAMETER_BLOCK_HASH).as_str(), 
                axum::routing::get(dataset_blocks_handler)
            )
            .route(
                format!("/data/:{}", APIServer::PARAMETER_PHYSICAL_HASH).as_str(), 
                axum::routing::get(dataset_data_handler)
            )
            .route(
                format!("/checkpoints/:{}", APIServer::PARAMETER_PHYSICAL_HASH).as_str(), 
                axum::routing::get(dataset_checkpoints_handler)
            )
            .route(
                "/pull", 
                axum::routing::get(dataset_pull_ws_upgrade_handler)
            )
            .route(
                "/push", 
                axum::routing::get(dataset_push_ws_upgrade_handler)
            )
            .layer(
                axum::extract::Extension(local_repo)
            );

        let app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/graphql",
                axum::routing::get(graphql_playground).post(graphql_handler),
            )
            .nest(
                format!(
                    "/:{}", 
                    APIServer::PARAMETER_DATASET_NAME
                ).as_str(), dataset_routes
            )
            .layer(
                tower::ServiceBuilder::new()
                    .layer(tower_http::trace::TraceLayer::new_for_http())
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    )
                    .layer(axum::extract::Extension(gql_schema)),
            );

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self { server }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), hyper::Error> {
        self.server.await
    }
}

async fn root() -> impl axum::response::IntoResponse {
    axum::response::Html(
        r#"
<h1>Kamu HTTP Server</h1>
<ul>
    <li><a href="/graphql">GraphQL Playground</li>
</ul>
"#,
    )
}

async fn graphql_handler(
    schema: axum::extract::Extension<kamu_adapter_graphql::Schema>,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

async fn dataset_refs_handler(
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> String {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    let ref_param = params.get(APIServer::PARAMETER_REF).unwrap();
    let block_ref = BlockRef::from_str(ref_param.as_str()).unwrap();
    let hash = dataset.as_metadata_chain().get_ref(&block_ref).await.unwrap();

    hash.to_string()
}

async fn dataset_blocks_handler(
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> Vec<u8> {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    let block_hash_param = params.get(APIServer::PARAMETER_BLOCK_HASH).unwrap();
    let block_hash = Multihash::from_multibase_str(block_hash_param.as_str()).unwrap();
    let block = dataset.as_metadata_chain().get_block(&block_hash).await.unwrap();
    
    let block_bytes = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    block_bytes.collapse_vec()
}

async fn dataset_data_handler(
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> axum::response::Response {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    let physical_hash_param = params.get(APIServer::PARAMETER_PHYSICAL_HASH).unwrap();
    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let data_stream = dataset.as_data_repo().get_stream(&physical_hash).await.unwrap();
    let body = axum_extra::body::AsyncReadBody::new(data_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

async fn dataset_checkpoints_handler(
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> axum::response::Response {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    let physical_hash_param = params.get(APIServer::PARAMETER_PHYSICAL_HASH).unwrap();
    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let checkpoint_stream = dataset.as_checkpoint_repo().get_stream(&physical_hash).await.unwrap();
    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> axum::response::Response {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    ws.on_upgrade(|socket| dataset_push_ws_handler(socket, dataset))
}

async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>
) -> axum::response::Response  {

    let dataset = resolve_dataset(local_dataset_repository, &params)
        .await
        .unwrap();

    ws.on_upgrade(|socket| dataset_pull_ws_handler(socket, dataset))
}

async fn dataset_push_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset: Arc<dyn Dataset>
) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };
        println!("Push client sent: {}", msg.to_text().unwrap());

        let reply = axum::extract::ws::Message::Text(String::from("Hi push client!"));
        if socket.send(reply).await.is_err() {
            // client disconnected
            return;
        }
    }        
}

async fn dataset_pull_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset: Arc<dyn Dataset>
) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };
        println!("Pull client sent: {}", msg.to_text().unwrap());

        let reply = axum::extract::ws::Message::Text(String::from("Hi pull client!"));
        if socket.send(reply).await.is_err() {
            // client disconnected
            return;
        }
    }        
}

async fn resolve_dataset(
    local_dataset_repository: axum::extract::Extension<Arc<dyn LocalDatasetRepository>>,
    params: &HashMap<String, String>
) -> Result<Arc<dyn Dataset>, GetDatasetError> {
    
    // TODO: support 'accountName' parameter
    let dataset_name_param = params.get(APIServer::PARAMETER_DATASET_NAME).unwrap();
    let dataset_name = DatasetName::from_str(dataset_name_param.as_str()).unwrap();
    let dataset_ref: DatasetRefLocal = DatasetRefLocal::Name(dataset_name);
    local_dataset_repository.get_dataset(&dataset_ref).await
}
