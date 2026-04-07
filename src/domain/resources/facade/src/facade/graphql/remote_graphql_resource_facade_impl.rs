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
use internal_error::{InternalError, ResultIntoInternal};
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

    fn apply_manifest_query(
        request: &ApplyManifestRequest,
        dry_run: bool,
    ) -> Result<String, ApplyManifestError> {
        let manifest = serde_json::to_string(&request.manifest).int_err()?;
        let format = request.format.to_string();

        Ok(format!(
            r#"
            mutation {{
              resources {{
                applyManifest(manifest: {manifest}, format: {format}, dryRun: {dry_run}) {{
                  __typename
                  ... on ResourceApplySuccess {{
                    operation
                    resource {{
                      apiVersion
                      kind {{
                        value
                      }}
                      metadata {{
                        id
                        accountId
                        name
                        description
                        labels
                        annotations
                        generation
                        createdAt
                        updatedAt
                        deletedAt
                        lastReconciledAt
                      }}
                      spec
                      status
                    }}
                    changes {{
                      kind
                      path
                      before
                      after
                    }}
                    warnings {{
                      code
                      path
                      message
                    }}
                  }}
                  ... on ResourceApplyRejection {{
                    category
                    message
                  }}
                }}
              }}
            }}
            "#
        ))
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
        unimplemented!("Remote resource facade get operation is not implemented yet")
    }

    async fn render_manifest(
        &self,
        _request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        unimplemented!("Remote resource facade render_manifest operation is not implemented yet")
    }

    async fn list(
        &self,
        _request: ListResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListResourcesError> {
        unimplemented!("Remote resource facade list operation is not implemented yet")
    }

    async fn list_all(
        &self,
        _request: ListAllResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListAllResourcesError> {
        unimplemented!("Remote resource facade list_all operation is not implemented yet")
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        let query = Self::apply_manifest_query(&request, true)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_planning_decision()
            .map_err(Into::into)
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
        let query = Self::apply_manifest_query(&request, false)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_application_decision()
            .map_err(Into::into)
    }

    async fn delete(
        &self,
        _request: DeleteResourceRequest,
    ) -> Result<kamu_resources::ResourceUID, DeleteResourceError> {
        unimplemented!("Remote resource facade delete operation is not implemented yet")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
