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
    ResourceListColumnDescriptor,
    ResourceListColumnValue,
    ResourceListColumnValueView,
    ResourcePhaseCounts,
    ResourceStatusSummaryView,
    ResourceSummaryView,
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
    GetResourceRef,
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
    const LIST_PAGE_SIZE: usize = 100;

    const LIST_BY_KIND_FIELDS: &'static str = r#"
        nodes {
          id
          apiVersion
          kind {
            value
          }
          name
          description
          generation
          createdAt
          updatedAt
          status {
            phase
            observedGeneration
            ready
          }
          listValues {
            key
            stringValue
            uint64Value
            boolValue
          }
        }
    "#;

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

    fn target_account_id<E>(
        account: Option<&domain::ResourceManifestAccount>,
    ) -> Result<Option<odf::AccountID>, E>
    where
        E: From<InternalError>,
    {
        match account {
            None => Ok(None),
            Some(account) => account.id.clone().map(Some).ok_or_else(|| {
                InternalError::new("Remote admin resource requests require target account id")
                    .into()
            }),
        }
    }

    fn parse_enum<T>(value: &str, field_name: &str) -> Result<T, InternalError>
    where
        T: std::str::FromStr,
    {
        value.parse().map_err(|_| {
            InternalError::new(format!(
                "Unsupported {field_name} '{value}' in remote resource list",
            ))
        })
    }

    fn resource_summary_from_fragment(
        fragment: fragments::ResourceSummaryFragment,
    ) -> Result<ResourceSummaryView, ListResourcesError> {
        let status = match fragment.status {
            Some(status) => Some(ResourceStatusSummaryView {
                phase: status
                    .phase
                    .as_deref()
                    .map(|phase| Self::parse_enum(phase, "resource phase"))
                    .transpose()?,
                observed_generation: status.observed_generation,
                ready: status.ready,
            }),
            None => None,
        };

        let list_values = fragment
            .list_values
            .into_iter()
            .map(|value| {
                let key = value.key;
                let value = match (value.string_value, value.uint64_value, value.bool_value) {
                    (Some(value), None, None) => ResourceListColumnValue::String(value),
                    (None, Some(value), None) => ResourceListColumnValue::UInt64(value),
                    (None, None, Some(value)) => ResourceListColumnValue::Bool(value),
                    (None, None, None) => {
                        return Err(ListResourcesError::Internal(InternalError::new(format!(
                            "Missing list value payload for key '{key}'",
                        ))));
                    }
                    _ => {
                        return Err(ListResourcesError::Internal(InternalError::new(format!(
                            "Ambiguous list value payload for key '{key}'",
                        ))));
                    }
                };

                Ok(ResourceListColumnValueView { key, value })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResourceSummaryView {
            kind: fragment.kind.value,
            api_version: fragment.api_version,
            uid: fragment.id,
            name: fragment.name,
            description: fragment.description,
            generation: fragment.generation,
            created_at: fragment.created_at,
            updated_at: fragment.updated_at,
            status,
            list_values,
        })
    }

    fn list_resources_query(
        kind: &str,
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, ListResourcesError> {
        let kind = serde_json::to_string(kind).int_err()?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_BY_KIND_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listByKind(kind: {{ custom: {kind} }}, page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_BY_KIND_FIELDS,
            )
        })
    }

    fn render_manifest_query(
        request: &RenderResourceManifestRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, RenderResourceManifestError> {
        let kind = serde_json::to_string(&request.kind).int_err()?;
        let selector_ref = Self::render_resource_ref_input(&request.resource_ref)?;
        let format = request.format.to_string();
        let maybe_api_version = match &request.api_version {
            Some(api_version) => {
                format!(
                    "apiVersion: {},",
                    serde_json::to_string(api_version).int_err()?
                )
            }
            None => String::new(),
        };

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      renderManifest(
                        selector: {{
                          kind: {{ custom: {kind} }},
                          {maybe_api_version}
                          ref: {selector_ref}
                        }}
                        format: {format}
                      ) {{
                        manifest
                        format
                      }}
                    }}
                  }}
                }}
                "#
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    renderManifest(
                      selector: {{
                        kind: {{ custom: {kind} }},
                        {maybe_api_version}
                        ref: {selector_ref}
                      }}
                      format: {format}
                    ) {{
                      manifest
                      format
                    }}
                  }}
                }}
                "#
            )
        })
    }

    fn render_resource_ref_input(
        resource_ref: &GetResourceRef,
    ) -> Result<String, RenderResourceManifestError> {
        match resource_ref {
            GetResourceRef::ById(uid) => Ok(format!(
                "{{ byId: {} }}",
                serde_json::to_string(&uid).int_err()?
            )),
            GetResourceRef::ByName(name) => Ok(format!(
                "{{ byName: {{ name: {} }} }}",
                serde_json::to_string(&name).int_err()?
            )),
        }
    }

    fn map_render_manifest_graphql_error(
        request: &RenderResourceManifestRequest,
        message: &str,
    ) -> Option<RenderResourceManifestError> {
        match &request.resource_ref {
            GetResourceRef::ById(uid) => {
                let error = domain::ResourceUIDNotFoundError(*uid);
                message
                    .contains(&error.to_string())
                    .then_some(RenderResourceManifestError::UIDNotFound(error))
            }
            GetResourceRef::ByName(name) => {
                let error = domain::ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                };
                message
                    .contains(&error.to_string())
                    .then_some(RenderResourceManifestError::NameNotFound(error))
            }
        }
    }

    fn map_render_manifest_remote_error(
        request: &RenderResourceManifestRequest,
        error: GraphqlHttpRequestError,
    ) -> RenderResourceManifestError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_render_manifest_graphql_error(request, &message).unwrap_or_else(|| {
                RenderResourceManifestError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                    endpoint_url,
                    message,
                })
            }),
            GraphqlHttpRequestError::Internal(error) => {
                RenderResourceManifestError::Internal(error)
            }
            other => RenderResourceManifestError::RemoteRequest(other),
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
                      listColumns {
                        key
                        header
                        dataType
                        visibility
                      }
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
            .map(|item| {
                Ok(ResourceKindDescriptor {
                    name: item.name,
                    short_names: item.short_names,
                    kind: item.kind.value,
                    api_version: item.api_version,
                    list_columns: item
                        .list_columns
                        .into_iter()
                        .map(|column| {
                            Ok(ResourceListColumnDescriptor {
                                key: column.key,
                                header: column.header,
                                data_type: Self::parse_enum(
                                    &column.data_type,
                                    "list column data type",
                                )?,
                                visibility: Self::parse_enum(
                                    &column.visibility,
                                    "list column visibility",
                                )?,
                            })
                        })
                        .collect::<Result<Vec<_>, InternalError>>()?,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()?)
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let maybe_target_account_id =
            Self::target_account_id::<ResourcesSummaryError>(request.account.as_ref())?;

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
        request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let maybe_target_account_id =
            Self::target_account_id::<RenderResourceManifestError>(request.account.as_ref())?;

        let query = Self::render_manifest_query(&request, maybe_target_account_id.as_ref())?;

        let rendered = if maybe_target_account_id.is_some() {
            let response: fragments::AdminRenderManifestQueryDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_render_manifest_remote_error(&request, error))?;
            response.admin.resources.render_manifest
        } else {
            let response: fragments::RenderManifestQueryDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_render_manifest_remote_error(&request, error))?;
            response.resources.render_manifest
        };

        Ok(RenderResourceManifestResult {
            manifest: rendered.manifest,
            format: rendered.format.into(),
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListResourcesError>(request.account.as_ref())?;

        let page = request
            .pagination
            .offset
            .checked_div(request.pagination.limit)
            .unwrap_or(0);
        let per_page = if request.pagination.limit == 0 {
            Self::LIST_PAGE_SIZE
        } else {
            request.pagination.limit
        };

        let query = Self::list_resources_query(
            &request.kind,
            page,
            per_page,
            maybe_target_account_id.as_ref(),
        )?;

        let nodes = if maybe_target_account_id.is_some() {
            let response: fragments::AdminListByKindQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.list_by_kind.nodes
        } else {
            let response: fragments::ListByKindQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.list_by_kind.nodes
        };

        nodes
            .into_iter()
            .map(Self::resource_summary_from_fragment)
            .collect()
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
