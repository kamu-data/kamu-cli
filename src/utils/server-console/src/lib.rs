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
    axum::response::Html(
        INDEX_HTML
            .replace("{{title}}", title)
            .replace("{{version}}", version),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn graphiql_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn graphql_playground_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const INDEX_HTML: &str = r###"
<html lang="en">
<head>
    <title>{{title}}</title>
    <style>
        body {
            background-color: #0d012f;
            color: #ddd;
            font-family: poppins,sans-serif;
        }

        a {
            text-decoration: none;
            color: #ddd;
            transition: color 0.3s ease-in-out;
        }
        a:hover {
            color: #ec6e2a;
        }

        li {
            font-size: large;
            padding: 0 0 5px 0;
        }

        .title {
            display: flex;
            align-items: center;
        }

        .icon {
            padding: 0 10px;
        }

        .title-version {
            font-size: small;
            vertical-align: top;
        }
    </style>
</head>
<body>
    <div class="title">
    <path class="icon">
        <svg width="51" height="44" viewBox="0 0 51 44" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path fill-rule="evenodd" clip-rule="evenodd" d="M21.4536 0L39.2361 30.8345H33.3086L21.4534 10.2781L3.67079 41.1127L0.707031 35.9736L21.4536 0Z" fill="#F99B81"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M21.4536 0L3.67079 41.1127L0.707031 35.9736L21.4536 0Z" fill="#F8CA51"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M21.4527 0L21.4526 10.2781L3.66992 41.1127L21.4527 0Z" fill="#F9AF54"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M21.4531 0L39.2356 30.8345H33.3081L21.4531 0Z" fill="#FA7789"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M50.4559 37.2178L14.8906 37.2177L17.8544 32.0786L41.5646 32.0787L23.7818 1.24414L29.7094 1.24418L50.4559 37.2178Z" fill="#1993C6"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M50.4534 37.2178L23.7793 1.24414L29.7069 1.24418L50.4534 37.2178Z" fill="#35D5D5"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M50.4534 37.2178L41.5621 32.0787L23.7793 1.24414L50.4534 37.2178Z" fill="#2CBED7"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M50.4559 37.2173L14.8906 37.2172L17.8544 32.0781L50.4559 37.2173Z" fill="#106FB0"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M3.85547 43.9984L21.6382 13.1641L24.602 18.3031L12.7468 38.8594L48.3123 38.8593L45.3485 43.9984L3.85547 43.9984Z" fill="#565BD8"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M3.85547 43.9984L48.3123 38.8594L45.3485 43.9985L3.85547 43.9984Z" fill="#DA76CB"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M3.85547 43.9984L12.7468 38.8595L48.3123 38.8594L3.85547 43.9984Z" fill="#A26BD2"/>
            <path fill-rule="evenodd" clip-rule="evenodd" d="M3.85547 43.9984L21.6382 13.1641L24.602 18.3031L3.85547 43.9984Z" fill="#2C45AB"/>
        </svg>
    </path>
    <h1>Kamu API Server <span class="title-version">{{version}}</span></h1>
    </div>
    <ul>
        <li><a href="/openapi">OpenAPI Playground</a></li>
        <li><a href="/graphql">GraphQL Playground</a></li>
        <ul>
            <li><a href="/graphql-legacy">Legacy version</a></li>
        </ul>
    </ul>
</body>
"###;
