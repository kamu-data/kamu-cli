// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn spec_handler(
    axum::Extension(api): axum::Extension<std::sync::Arc<utoipa::openapi::OpenApi>>,
) -> impl axum::response::IntoResponse {
    axum::Json(api)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn ui_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(include_str!("../../resources/openapi/scalar.html"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/openapi.json", axum::routing::get(spec_handler))
        .route("/openapi", axum::routing::get(ui_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn spec_builder(version: &str, description: &str) -> utoipa::openapi::OpenApiBuilder {
    use utoipa::openapi::security::*;
    use utoipa::openapi::tag::*;
    use utoipa::openapi::*;

    OpenApiBuilder::new()
        .info(
            InfoBuilder::new()
                .title("Kamu REST API")
                .version(version)
                .terms_of_service(Some("https://docs.kamu.dev/terms-of-service/"))
                .license(Some(
                    LicenseBuilder::new()
                        .name("BSL")
                        .url(Some("https://docs.kamu.dev/license/"))
                        .build(),
                ))
                .description(Some(description)),
        )
        .tags(Some([
            TagBuilder::new()
                .name("odf-core")
                .description(Some(indoc::indoc!(
                    r#"
                    Core ODF APIs.

                    [Open Data Fabric](https://docs.kamu.dev/odf/) (ODF) is a specification for
                    open data formats and protocols for interoperable data exchange and processing.
                    Kamu is just one possible implementation of this spec. APIs in this group are
                    part of the spec and thus should be supported by most ODF implementations.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("odf-transfer")
                .description(Some(indoc::indoc!(
                    r#"
                    ODF Data Transfer APIs.

                    This group includes two main protocols:

                    - [Simple Transfer Protocol](https://docs.kamu.dev/odf/spec/#simple-transfer-protocol)
                    (SiTP) is a bare-minimum read-only protocol used for synchronizing datasets between
                    repositories. It is simple to implement and support, but accesses metadata blocks and
                    other dataset components on individual object basis which may be inefficient for
                    large datasets.

                    - [Smart Transfer Protocol](https://docs.kamu.dev/odf/spec/#smart-transfer-protocol)
                    (SmTP) is an extension of SiTP that adds efficient batch transfer of small objects,
                    compression, and can proxy reads and writes of large objects directly to underlying
                    storage.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("odf-query")
                .description(Some(indoc::indoc!(
                    r#"
                    ODF Data Query APIs.

                    APIs in this group allow to query data and metadata of datasets and generate
                    cryptographic proofs for the results.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("kamu")
                .description(Some(indoc::indoc!(
                    r#"
                    General Node APIs.

                    APIs in this group are either Kamu-specific or are experimental before they
                    are included into ODF spec.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("kamu-odata")
                .description(Some(indoc::indoc!(
                    r#"
                    OData Adapter.

                    This group f APIs represente an [OData](https://learn.microsoft.com/en-us/odata/overview)
                    protocol adapter on top of the ODF query API. See
                    [datafusion-odata](https://github.com/kamu-data/datafusion-odata/) library for details
                    on what parts of the OData protocol are currently supported.
                    "#
                )))
                .build(),
        ]))
        .components(Some(
            ComponentsBuilder::new()
                .security_scheme(
                    "api_key",
                    SecurityScheme::Http(
                        HttpBuilder::new()
                            .scheme(HttpAuthScheme::Bearer)
                            .bearer_format("AccessToken")
                            .build(),
                    ),
                )
                .build(),
        ))
}
