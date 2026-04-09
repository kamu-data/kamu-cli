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
    ResourceNameNotFoundError,
    ResourcePhaseCounts,
    ResourceStatusSummaryView,
    ResourceSummaryView,
    ResourceTypeCountSummary,
    ResourceUIDNotFoundError,
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
    ResourceKindMismatchError,
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

    const LIST_FIELDS: &'static str = r#"
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

    fn resource_summary_from_fragment<E>(
        fragment: fragments::ResourceSummaryFragment,
    ) -> Result<ResourceSummaryView, E>
    where
        E: From<InternalError>,
    {
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
                        return Err(E::from(InternalError::new(format!(
                            "Missing list value payload for key '{key}'",
                        ))));
                    }
                    _ => {
                        return Err(E::from(InternalError::new(format!(
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
    ) -> Result<String, InternalError> {
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
                fields = Self::LIST_FIELDS,
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
                fields = Self::LIST_FIELDS,
            )
        })
    }

    fn list_all_resources_query(
        page: usize,
        per_page: usize,
        target_account_id: Option<&odf::AccountID>,
    ) -> String {
        if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      listAll(page: {page}, perPage: {per_page}) {{
                        {fields}
                      }}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        } else {
            format!(
                r#"
                query {{
                  resources {{
                    listAll(page: {page}, perPage: {per_page}) {{
                      {fields}
                    }}
                  }}
                }}
                "#,
                fields = Self::LIST_FIELDS,
            )
        }
    }

    fn graphql_page_params(offset: usize, limit: usize) -> (usize, usize) {
        let page = offset.checked_div(limit).unwrap_or(0);
        let per_page = if limit == 0 {
            Self::LIST_PAGE_SIZE
        } else {
            limit
        };

        (page, per_page)
    }

    async fn list_resource_summaries<E>(
        &self,
        query_kind: RemoteListQuery<'_>,
        maybe_target_account_id: Option<&odf::AccountID>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceSummaryView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = Self::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                Self::list_resources_query(kind, page, per_page, maybe_target_account_id)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                Self::list_all_resources_query(page, per_page, maybe_target_account_id)
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_by_kind.nodes
            }
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_by_kind.nodes
            }
            RemoteListQuery::All if maybe_target_account_id.is_some() => {
                let response: fragments::AdminListAllQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.admin.resources.list_all.nodes
            }
            RemoteListQuery::All => {
                let response: fragments::ListAllQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_all.nodes
            }
        };

        nodes
            .into_iter()
            .map(Self::resource_summary_from_fragment::<E>)
            .collect()
    }

    fn selector_input(
        kind: &str,
        api_version: Option<&str>,
        resource_ref: &GetResourceRef,
    ) -> Result<String, InternalError> {
        let kind = serde_json::to_string(kind).int_err()?;
        let selector_ref = Self::resource_ref_input(resource_ref)?;
        let maybe_api_version = match api_version {
            Some(api_version) => format!(
                "apiVersion: {},",
                serde_json::to_string(api_version).int_err()?
            ),
            None => String::new(),
        };

        Ok(format!(
            r#"{{
                kind: {{ custom: {kind} }},
                {maybe_api_version}
                ref: {selector_ref}
            }}"#
        ))
    }

    fn get_resource_query(
        request: &GetResourceRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, GetResourceError> {
        let selector = Self::selector_input(
            &request.kind,
            request.api_version.as_deref(),
            &request.resource_ref,
        )?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      resource(selector: {selector}) {{
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
                    resource(selector: {selector}) {{
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
                  }}
                }}
                "#
            )
        })
    }

    fn render_manifest_query(
        request: &RenderResourceManifestRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, RenderResourceManifestError> {
        let selector = Self::selector_input(
            &request.kind,
            request.api_version.as_deref(),
            &request.resource_ref,
        )
        .map_err(RenderResourceManifestError::Internal)?;
        let format = request.format.to_string();

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                query {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      renderManifest(
                        selector: {selector}
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
                      selector: {selector}
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

    fn delete_resource_query(
        request: &DeleteResourceRequest,
        target_account_id: Option<&odf::AccountID>,
    ) -> Result<String, DeleteResourceError> {
        let selector = Self::selector_input(&request.kind, None, &request.resource_ref)
            .map_err(DeleteResourceError::Internal)?;

        Ok(if let Some(target_account_id) = target_account_id {
            format!(
                r#"
                mutation {{
                  admin {{
                    resources(accountId: "{target_account_id}") {{
                      delete(selector: {selector}) {{
                        resourceId
                      }}
                    }}
                  }}
                }}
                "#
            )
        } else {
            format!(
                r#"
                mutation {{
                  resources {{
                    delete(selector: {selector}) {{
                      resourceId
                    }}
                  }}
                }}
                "#
            )
        })
    }

    fn resource_ref_input(resource_ref: &GetResourceRef) -> Result<String, InternalError> {
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

    fn map_delete_graphql_error(
        request: &DeleteResourceRequest,
        message: &str,
    ) -> Option<DeleteResourceError> {
        match &request.resource_ref {
            GetResourceRef::ById(uid) => {
                let not_found = domain::ResourceUIDNotFoundError(*uid);
                if message.contains(&not_found.to_string()) {
                    return Some(DeleteResourceError::UIDNotFound(not_found));
                }

                let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
                let mismatch_suffix = format!("', expected '{}'", request.kind);
                if let Some(actual_kind_start) = message.find(&mismatch_prefix) {
                    let actual_kind_start = actual_kind_start + mismatch_prefix.len();
                    if let Some(actual_kind_end) =
                        message[actual_kind_start..].find(&mismatch_suffix)
                    {
                        return Some(DeleteResourceError::KindMismatch(
                            ResourceKindMismatchError {
                                uid: *uid,
                                expected_kind: request.kind.clone(),
                                actual_kind: message
                                    [actual_kind_start..actual_kind_start + actual_kind_end]
                                    .to_string(),
                            },
                        ));
                    }
                }

                None
            }
            GetResourceRef::ByName(name) => {
                let not_found = domain::ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                };
                message
                    .contains(&not_found.to_string())
                    .then_some(DeleteResourceError::NameNotFound(not_found))
            }
        }
    }

    fn map_delete_remote_error(
        request: &DeleteResourceRequest,
        error: GraphqlHttpRequestError,
    ) -> DeleteResourceError {
        match error {
            GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            } => Self::map_delete_graphql_error(request, &message).unwrap_or_else(|| {
                DeleteResourceError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                    endpoint_url,
                    message,
                })
            }),
            GraphqlHttpRequestError::Internal(error) => DeleteResourceError::Internal(error),
            other => DeleteResourceError::RemoteRequest(other),
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

    fn not_found_error(request: &GetResourceRequest) -> GetResourceError {
        match &request.resource_ref {
            GetResourceRef::ById(uid) => {
                GetResourceError::UIDNotFound(ResourceUIDNotFoundError(*uid))
            }
            GetResourceRef::ByName(name) => {
                GetResourceError::NameNotFound(ResourceNameNotFoundError {
                    kind: request.kind.clone(),
                    name: name.clone(),
                })
            }
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
        request: GetResourceRequest,
    ) -> Result<kamu_resources::ResourceView, GetResourceError> {
        let maybe_target_account_id =
            Self::target_account_id::<GetResourceError>(request.account.as_ref())?;

        let query = Self::get_resource_query(&request, maybe_target_account_id.as_ref())?;

        let maybe_resource = if maybe_target_account_id.is_some() {
            let response: fragments::AdminGetResourceQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.admin.resources.resource
        } else {
            let response: fragments::GetResourceQueryDataFragment =
                self.execute_graphql(&query).await?;
            response.resources.resource
        };

        let Some(resource) = maybe_resource else {
            return Err(Self::not_found_error(&request));
        };

        resource.try_into().map_err(GetResourceError::Internal)
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

        self.list_resource_summaries::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<kamu_resources::ResourceSummaryView>, ListAllResourcesError> {
        let maybe_target_account_id =
            Self::target_account_id::<ListAllResourcesError>(request.account.as_ref())?;

        self.list_resource_summaries::<ListAllResourcesError>(
            RemoteListQuery::All,
            maybe_target_account_id.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
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
        request: DeleteResourceRequest,
    ) -> Result<kamu_resources::ResourceUID, DeleteResourceError> {
        let maybe_target_account_id =
            Self::target_account_id::<DeleteResourceError>(request.account.as_ref())?;

        let query = Self::delete_resource_query(&request, maybe_target_account_id.as_ref())?;

        let resource_id = if maybe_target_account_id.is_some() {
            let response: fragments::AdminDeleteMutationDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_delete_remote_error(&request, error))?;
            response.admin.resources.delete.resource_id
        } else {
            let response: fragments::DeleteMutationDataFragment = self
                .execute_graphql(&query)
                .await
                .map_err(|error| Self::map_delete_remote_error(&request, error))?;
            response.resources.delete.resource_id
        };

        Ok(resource_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum RemoteListQuery<'a> {
    ByKind(&'a str),
    All,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
