// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn router(title: String, version: String) -> axum::Router {
    axum::Router::new()
        .route(
            "/",
            axum::routing::get(async move || index_handler(&title, &version).await),
        )
        .route("/graphql", axum::routing::get(graphiql_handler))
        .route(
            "/graphql-legacy",
            axum::routing::get(graphql_playground_handler),
        )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
async fn index_handler(title: &str, version: &str) -> impl axum::response::IntoResponse {
    let html = include_str!("../assets/index.template.html");

    axum::response::Html(
        html.replace("{{title}}", title)
            .replace("{{version}}", version),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn graphiql_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/graphql")
            .finish()
            // TODO: FIXME: Remove this hack after upstream issue is fixed
            // See: https://github.com/async-graphql/async-graphql/issues/1703
            .replace(
                "https://unpkg.com/graphiql/",
                "https://unpkg.com/graphiql@3.9.0/",
            ),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn graphql_playground_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
