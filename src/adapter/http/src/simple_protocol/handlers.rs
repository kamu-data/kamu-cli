// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{DatasetRef, Multihash};
use url::Url;

use crate::axum_utils::*;
use crate::smart_protocol::{
    AxumServerPullProtocolInstance,
    AxumServerPushProtocolInstance,
    BearerHeader,
};

/////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
pub struct RefFromPath {
    reference: String,
}

#[derive(serde::Deserialize)]
pub struct BlockHashFromPath {
    block_hash: Multihash,
}

#[derive(serde::Deserialize)]
pub struct PhysicalHashFromPath {
    physical_hash: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_refs_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(ref_param): axum::extract::Path<RefFromPath>,
) -> Result<String, axum::response::Response> {
    let block_ref = match BlockRef::from_str(&ref_param.reference.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(_) => Err(not_found_response()),
    }?;

    let get_ref_result = dataset.as_metadata_chain().get_ref(&block_ref).await;

    match get_ref_result {
        Ok(hash) => Ok(hash.to_string()),
        Err(GetRefError::NotFound(_)) => Err(not_found_response()),
        Err(_) => {
            tracing::debug!(
                reference = %ref_param.reference,
                "Internal error while resolving reference"
            );
            return Err(internal_server_error_response());
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_blocks_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<BlockHashFromPath>,
) -> Result<Vec<u8>, axum::response::Response> {
    let block = match dataset
        .as_metadata_chain()
        .get_block(&hash_param.block_hash)
        .await
    {
        Ok(block) => block,
        Err(GetBlockError::NotFound(_)) => return Err(not_found_response()),
        Err(e) => {
            tracing::debug!(block_hash = %hash_param.block_hash, "GetBlockError: {}", e);
            return Err(internal_server_error_response());
        }
    };

    match FlatbuffersMetadataBlockSerializer.write_manifest(&block) {
        Ok(block_bytes) => Ok(block_bytes.collapse_vec()),
        Err(e) => {
            tracing::debug!(block_hash = %hash_param.block_hash, "Block serialization failed: {}", e);
            Err(internal_server_error_response())
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> axum::response::Response {
    let data_stream = match dataset
        .as_data_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return not_found_response(),
        Err(e) => {
            tracing::debug!(physical_hash = %hash_param.physical_hash, "Data GetError: {}", e);
            return internal_server_error_response();
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(data_stream);
    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> axum::response::Response {
    let checkpoint_stream = match dataset
        .as_checkpoint_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return not_found_response(),
        Err(e) => {
            tracing::debug!(physical_hash = %hash_param.physical_hash, "Checkpoint GetError: {}", e);
            return internal_server_error_response();
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);
    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    axum::TypedHeader(content_length): axum::TypedHeader<axum::headers::ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    dataset_put_object_common(
        dataset.as_data_repo(),
        hash_param.physical_hash,
        content_length.0 as usize,
        body_stream,
    )
    .await
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    axum::TypedHeader(content_length): axum::TypedHeader<axum::headers::ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    dataset_put_object_common(
        dataset.as_checkpoint_repo(),
        hash_param.physical_hash,
        content_length.0 as usize,
        body_stream,
    )
    .await
}

/////////////////////////////////////////////////////////////////////////////////

async fn dataset_put_object_common(
    object_repository: &dyn ObjectRepository,
    physical_hash: Multihash,
    content_length: usize,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    let src = Box::new(crate::axum_utils::body_into_async_read(body_stream));

    object_repository
        .insert_stream(
            src,
            InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&physical_hash),
                size_hint: Some(content_length),
            },
        )
        .await
        .map_err(|_| internal_server_error_response())?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset_ref): axum::extract::Extension<DatasetRef>,
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
    maybe_bearer_header: Option<BearerHeader>,
) -> axum::response::Response {
    let dataset_url = get_base_dataset_url(host, uri, 1);

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    let dataset = match dataset_repo.get_dataset(&dataset_ref).await {
        Ok(ds) => Some(ds),
        Err(GetDatasetError::NotFound(_)) => {
            let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
            match current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => return unauthorized_access_response(),
                CurrentAccountSubject::Logged(l) => {
                    // Make sure account in dataset ref being created and token account match
                    if let Some(ref_account_name) = dataset_ref.account_name() {
                        if ref_account_name != &l.account_name {
                            return forbidden_access_response();
                        }
                    }
                }
            }
            None
        }
        Err(err) => {
            tracing::error!("Could not get dataset: {:?}", err);
            return internal_server_error_response();
        }
    };

    ws.on_upgrade(|socket| {
        AxumServerPushProtocolInstance::new(
            socket,
            dataset_repo,
            dataset_ref,
            dataset,
            dataset_url,
            maybe_bearer_header,
        )
        .serve()
    })
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
    maybe_bearer_header: Option<BearerHeader>,
) -> axum::response::Response {
    let dataset_url = get_base_dataset_url(host, uri, 1);

    ws.on_upgrade(move |socket| {
        AxumServerPullProtocolInstance::new(socket, dataset, dataset_url, maybe_bearer_header)
            .serve()
    })
}

/////////////////////////////////////////////////////////////////////////////////

fn get_base_dataset_url(
    axum::extract::Host(host): axum::extract::Host,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    depth: usize,
) -> Url {
    let api_server_url = get_api_server_url(host);

    let mut path: Vec<_> = uri.path().split('/').collect();
    for _ in 0..depth {
        path.pop();
    }
    let path_string = format!("{}/", path.join("/"));
    api_server_url.join(path_string.as_str()).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

fn get_api_server_url(host: String) -> Url {
    let scheme = std::env::var("KAMU_PROTOCOL_SCHEME").unwrap_or_else(|_| String::from("http"));
    Url::parse(&format!("{}://{}", scheme, host)).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////
