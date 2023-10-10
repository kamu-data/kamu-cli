// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::simple_protocol::*;
use crate::{DatasetAuthorizationLayer, DatasetResolverLayer};

/////////////////////////////////////////////////////////////////////////////////

// Extractor of dataset identity for single-tenant smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: opendatafabric::DatasetName,
}

// Extractor of account + dataset identity for multi-tenant smart transfer
// protocol
#[derive(serde::Deserialize)]
struct DatasetByAccountAndName {
    account_name: opendatafabric::AccountName,
    dataset_name: opendatafabric::DatasetName,
}

/////////////////////////////////////////////////////////////////////////////////

pub fn smart_transfer_protocol_router() -> axum::Router {
    axum::Router::new()
        .route("/refs/:reference", axum::routing::get(dataset_refs_handler))
        .route(
            "/blocks/:block_hash",
            axum::routing::get(dataset_blocks_handler),
        )
        .route(
            "/data/:physical_hash",
            axum::routing::get(dataset_data_get_handler).put(dataset_data_put_handler),
        )
        .route(
            "/checkpoints/:physical_hash",
            axum::routing::get(dataset_checkpoints_get_handler)
                .put(dataset_checkpoints_put_handler),
        )
        .route("/pull", axum::routing::get(dataset_pull_ws_upgrade_handler))
        .route("/push", axum::routing::get(dataset_push_ws_upgrade_handler))
        .layer(DatasetAuthorizationLayer::new(
            get_dataset_action_for_request,
        ))
}

/////////////////////////////////////////////////////////////////////////////////

pub fn add_dataset_resolver_layer(
    dataset_router: axum::Router,
    multi_tenant: bool,
) -> axum::Router {
    use axum::extract::Path;

    if multi_tenant {
        dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByAccountAndName>| {
                opendatafabric::DatasetAlias::new(Some(p.account_name), p.dataset_name)
                    .into_local_ref()
            },
            is_dataset_optional_for_request,
        ))
    } else {
        dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByName>| p.dataset_name.as_local_ref(),
            is_dataset_optional_for_request,
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////

fn is_dataset_optional_for_request(request: &http::Request<hyper::Body>) -> bool {
    request.uri().path() == "/push"
}

/////////////////////////////////////////////////////////////////////////////////

fn get_dataset_action_for_request(
    request: &http::Request<hyper::Body>,
) -> kamu::domain::auth::DatasetAction {
    if !request.method().is_safe() || request.uri().path() == "/push" {
        kamu::domain::auth::DatasetAction::Write
    } else {
        kamu::domain::auth::DatasetAction::Read
    }
}

/////////////////////////////////////////////////////////////////////////////////
