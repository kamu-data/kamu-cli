// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ws_smart_transfer_protocol_axum_server;
use axum::{
    extract::{Extension, Path},
    headers::Host,
    TypedHeader,
};
use kamu::domain::{BlockRef, Dataset, GetDatasetError, LocalDatasetRepository};

use opendatafabric::{
    serde::{flatbuffers::FlatbuffersMetadataBlockSerializer, MetadataBlockSerializer},
    DatasetName, DatasetRefLocal, Multihash,
};
use std::{str::FromStr, sync::Arc};
use url::Url;

pub async fn dataset_refs_handler(
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path((dataset_name_param, ref_param)): Path<(String, String)>,
    TypedHeader(host): TypedHeader<Host>,
) -> String {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    let block_ref = BlockRef::from_str(ref_param.as_str()).unwrap();
    let hash = dataset
        .as_metadata_chain()
        .get_ref(&block_ref)
        .await
        .unwrap();

    hash.to_string()
}

pub async fn dataset_blocks_handler(
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path((dataset_name_param, block_hash_param)): Path<(String, String)>,
    TypedHeader(host): TypedHeader<Host>,
) -> Vec<u8> {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    let block_hash = Multihash::from_multibase_str(block_hash_param.as_str()).unwrap();
    let block = dataset
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap();

    let block_bytes = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    block_bytes.collapse_vec()
}

pub async fn dataset_data_handler(
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path((dataset_name_param, physical_hash_param)): Path<(String, String)>,
    TypedHeader(host): TypedHeader<Host>,
) -> axum::response::Response {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let data_stream = dataset
        .as_data_repo()
        .get_stream(&physical_hash)
        .await
        .unwrap();
    let body = axum_extra::body::AsyncReadBody::new(data_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

pub async fn dataset_checkpoints_handler(
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path((dataset_name_param, physical_hash_param)): Path<(String, String)>,
    TypedHeader(host): TypedHeader<Host>,
) -> axum::response::Response {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let checkpoint_stream = dataset
        .as_checkpoint_repo()
        .get_stream(&physical_hash)
        .await
        .unwrap();
    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path(dataset_name_param): Path<String>,
    TypedHeader(host): TypedHeader<Host>,
) -> axum::response::Response {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    ws.on_upgrade(|socket| {
        ws_smart_transfer_protocol_axum_server::dataset_push_ws_handler(socket, dataset)
    })
}

pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    Path(dataset_name_param): Path<String>,
    TypedHeader(host): TypedHeader<Host>,
) -> axum::response::Response {
    let dataset = resolve_dataset(local_dataset_repository, &dataset_name_param, host)
        .await
        .unwrap();

    ws.on_upgrade(move |socket| {
        ws_smart_transfer_protocol_axum_server::dataset_pull_ws_handler(socket, dataset)
    })
}

async fn resolve_dataset(
    local_dataset_repository: Extension<Arc<dyn LocalDatasetRepository>>,
    dataset_name_param: &String,
    api_host: Host,
) -> Result<Arc<dyn Dataset>, GetDatasetError> {
    let mut base_url_str = String::from("http://");
    base_url_str += api_host.hostname();
    if let Some(port) = api_host.port() {
        base_url_str += ":";
        base_url_str += &port.to_string();
    }
    base_url_str += "/";
    base_url_str += dataset_name_param.as_str();
    base_url_str += "/";

    let base_url = Url::parse(base_url_str.as_str()).unwrap();

    // TODO: support 'accountName' parameter
    let dataset_name = DatasetName::from_str(dataset_name_param.as_str()).unwrap();
    let dataset_ref: DatasetRefLocal = DatasetRefLocal::Name(dataset_name);
    local_dataset_repository
        .get_dataset_with_external_url(&dataset_ref, base_url)
        .await
}
