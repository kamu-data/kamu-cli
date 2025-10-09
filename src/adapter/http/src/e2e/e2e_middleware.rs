// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::Method;
use http_common::ApiError;
use messaging_outbox::OutboxAgent;
use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Middleware that invokes Outbox messages processing for mutable requests
pub async fn e2e_middleware_fn(request: Request, next: Next) -> Result<Response, ApiError> {
    let base_catalog = request
        .extensions()
        .get::<dill::Catalog>()
        .cloned()
        .expect("Catalog not found in http server extensions");

    let (is_mutable_request, request) = analyze_request_for_mutability(request).await?;
    let response = next.run(request).await;

    if is_mutable_request && response.status().is_success() {
        let outbox_agent = base_catalog.get_one::<dyn OutboxAgent>().unwrap();
        outbox_agent.run_while_has_tasks().await?;
    }

    Ok(response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn analyze_request_for_mutability(request: Request) -> Result<(bool, Request), ApiError> {
    {
        let is_not_modifying_requests = request.method() != Method::POST;

        if is_not_modifying_requests {
            return Ok((false, request));
        }
    }
    {
        let is_rest_api_post_request = request.uri().path() != "/graphql";

        if is_rest_api_post_request {
            return Ok((true, request));
        }
    }
    {
        // In the case of GQL, we check whether the query is mutable or not
        let (request_parts, request_body) = request.into_parts();
        let buffered_request_body = buffer_request_body(request_body).await?;

        let is_mutating_gql = if let Ok(body) = std::str::from_utf8(&buffered_request_body) {
            let gql_request = serde_json::from_str::<SimplifiedGqlRequest>(body)
                .map_err(ApiError::bad_request)?;

            gql_request.query.starts_with("mutation")
        } else {
            false
        };

        let request = Request::from_parts(request_parts, Body::from(buffered_request_body));

        Ok((is_mutating_gql, request))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn buffer_request_body<B>(request_body: B) -> Result<Bytes, ApiError>
where
    B: axum::body::HttpBody<Data = Bytes>,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    use http_body_util::BodyExt;

    let body_bytes = match request_body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return Err(ApiError::bad_request(e));
        }
    };

    Ok(body_bytes)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
struct SimplifiedGqlRequest {
    query: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
