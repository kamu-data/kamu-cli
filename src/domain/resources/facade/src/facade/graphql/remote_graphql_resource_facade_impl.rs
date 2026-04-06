// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use internal_error::{InternalError, ResultIntoInternal};
use serde::Deserialize;
use url::Url;

use super::remote_graphql_resource_facade_fragments as fragments;
use crate::{
    ApplyManifestApplicationDecision,
    ApplyManifestError,
    ApplyManifestPlanningDecision,
    ApplyManifestRequest,
    DeleteResourceError,
    DeleteResourceRequest,
    GetResourceError,
    GetResourceRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    RenderResourceManifestResult,
    ResourceFacade,
    ResourceKindDescriptor,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: intentionally not a dill component, used via factories
pub struct RemoteGraphqlResourceFacadeImpl {
    graphql_endpoint_url: Url,
    http_client: reqwest::Client,
    maybe_access_token: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteGraphqlResourceFacadeImpl {
    pub fn new(backend_url: &Url, maybe_access_token: Option<String>) -> Self {
        Self {
            graphql_endpoint_url: Self::graphql_endpoint_url(backend_url),
            http_client: reqwest::Client::new(),
            maybe_access_token,
        }
    }

    fn graphql_endpoint_url(backend_url: &Url) -> Url {
        let mut graphql_endpoint_url = backend_url.clone();
        let path = graphql_endpoint_url.path().trim_end_matches('/');
        let path = if path.is_empty() {
            "/graphql".to_string()
        } else {
            format!("{path}/graphql")
        };
        graphql_endpoint_url.set_path(&path);
        graphql_endpoint_url.set_query(None);
        graphql_endpoint_url.set_fragment(None);
        graphql_endpoint_url
    }

    async fn execute_graphql<T>(&self, query: &str) -> Result<T, InternalError>
    where
        T: serde::de::DeserializeOwned,
    {
        let body = serde_json::to_vec(&serde_json::json!({ "query": query })).int_err()?;

        let mut request = self
            .http_client
            .post(self.graphql_endpoint_url.clone())
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .body(body);

        if let Some(access_token) = self.maybe_access_token.as_ref() {
            request = request.header(AUTHORIZATION, format!("Bearer {access_token}"));
        }

        let response = request.send().await.int_err()?;
        let status = response.status();
        let response_body = response.text().await.int_err()?;

        if !status.is_success() {
            return Err(InternalError::new(format!(
                "Remote GraphQL request to '{}' failed: {} {}",
                self.graphql_endpoint_url,
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown status")
            )));
        }

        let response: GraphqlResponseFragment<T> = serde_json::from_str(&response_body)
            .int_err()
            .map_err(|e| {
            e.with_context(format!(
                "Failed to deserialize remote GraphQL response from '{}'",
                self.graphql_endpoint_url
            ))
        })?;

        if let Some(errors) = response.errors
            && let Some(error) = errors.first()
        {
            return Err(InternalError::new(format!(
                "Remote GraphQL request to '{}' failed: {}",
                self.graphql_endpoint_url, error.message
            )));
        }

        response.data.ok_or_else(|| {
            InternalError::new(format!(
                "Remote GraphQL request to '{}' returned no data",
                self.graphql_endpoint_url
            ))
        })
    }

    fn unsupported_operation_error() -> InternalError {
        InternalError::new("Remote resource facade operation is not implemented yet")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        let response: fragments::SupportedKindsQueryDataFragment = self
            .execute_graphql(
                r#"
                query {
                  resources {
                    supportedKinds {
                      name
                      shortNames
                      kind {
                        value
                      }
                      apiVersion
                    }
                  }
                }
                "#,
            )
            .await?;

        Ok(response
            .resources
            .supported_kinds
            .into_iter()
            .map(|item| ResourceKindDescriptor {
                name: item.name,
                short_names: item.short_names,
                kind: item.kind.value,
                api_version: item.api_version,
            })
            .collect())
    }

    async fn plan_apply_manifest(
        &self,
        _request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn apply_manifest(
        &self,
        _request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn get(
        &self,
        _request: GetResourceRequest,
    ) -> Result<kamu_resources::ResourceView, GetResourceError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn render_manifest(
        &self,
        _request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn list(
        &self,
        _request: ListResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListResourcesError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn list_all(
        &self,
        _request: ListAllResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListAllResourcesError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn delete(
        &self,
        _request: DeleteResourceRequest,
    ) -> Result<kamu_resources::ResourceUID, DeleteResourceError> {
        Err(Self::unsupported_operation_error().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
struct GraphqlResponseFragment<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Option<Vec<GraphqlErrorFragment>>,
}

#[derive(Debug, Deserialize)]
struct GraphqlErrorFragment {
    message: String,
}
