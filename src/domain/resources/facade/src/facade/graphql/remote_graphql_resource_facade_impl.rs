// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use domain::{
    ResourceKindDescriptor,
    ResourcePhaseCounts,
    ResourceTypeCountSummary,
    ResourcesSummary,
};
use graphql_http::{GraphqlHttpClient, GraphqlHttpRequestError};
use internal_error::InternalError;
use kamu_resources as domain;
use url::Url;

use super::fragments;
use crate::{
    ApplyManifestError,
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
    ResourcesSummaryError,
    ResourcesSummaryRequest,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: intentionally not a dill component, used via factories
pub struct RemoteGraphqlResourceFacadeImpl {
    graphql_client: GraphqlHttpClient,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteGraphqlResourceFacadeImpl {
    pub fn new(backend_url: &Url, maybe_access_token: Option<String>) -> Self {
        Self {
            graphql_client: GraphqlHttpClient::from_backend_url(backend_url, maybe_access_token),
        }
    }

    async fn execute_graphql<T>(&self, query: &str) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.graphql_client.execute(query).await
    }

    fn unsupported_operation_error() -> InternalError {
        InternalError::new("Remote resource facade operation is not implemented yet")
    }

    fn target_account_id(
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<Option<odf::AccountID>, ResourcesSummaryError> {
        match account {
            None => Ok(None),
            Some(account) => account.id.clone().map(Some).ok_or_else(|| {
                ResourcesSummaryError::Internal(InternalError::new(
                    "Remote admin resource summary requires target account id",
                ))
            }),
        }
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

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let maybe_target_account_id = Self::target_account_id(request.account.as_ref())?;

        let response = if let Some(target_account_id) = maybe_target_account_id {
            let query = format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      summary {{
                        resourceCounts {{
                          kind
                          name
                          apiVersion
                          totalCount
                          phaseCounts {{
                            pending
                            reconciling
                            ready
                            degraded
                            failed
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
                "#
            );

            let response: fragments::AdminSummaryQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.summary
        } else {
            let response: fragments::SummaryQueryDataFragment = self
                .execute_graphql(
                    r#"
                    query {
                      resources {
                        summary {
                          resourceCounts {
                            kind
                            name
                            apiVersion
                            totalCount
                            phaseCounts {
                              pending
                              reconciling
                              ready
                              degraded
                              failed
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .await?;
            response.resources.summary
        };

        Ok(ResourcesSummary {
            resource_counts: response
                .resource_counts
                .into_iter()
                .map(|item| ResourceTypeCountSummary {
                    kind: item.kind,
                    name: item.name,
                    api_version: item.api_version,
                    total_count: item.total_count,
                    phase_counts: ResourcePhaseCounts {
                        pending: item.phase_counts.pending,
                        reconciling: item.phase_counts.reconciling,
                        ready: item.phase_counts.ready,
                        degraded: item.phase_counts.degraded,
                        failed: item.phase_counts.failed,
                    },
                })
                .collect(),
        })
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

    async fn plan_apply_manifest(
        &self,
        _request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        Err(Self::unsupported_operation_error().into())
    }

    async fn apply_manifest(
        &self,
        _request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
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
