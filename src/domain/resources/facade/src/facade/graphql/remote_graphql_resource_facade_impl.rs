// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::{GraphqlHttpClient, GraphqlHttpRequestError};
use internal_error::InternalError;
use kamu_resources as domain;
use kamu_resources::{
    ResourceIdentityView,
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
use url::Url;

use super::{error_mapper, fragments, query_builder};
use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    BatchResourceResponse,
    BatchResourceSuccess,
    DeleteResourceError,
    GetResourceError,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceSelector,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourceIdentitiesRequest,
    SearchResourceIdentitiesResponse,
    SpecViewMode,
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
                    .map(|phase| query_builder::parse_enum(phase, "resource phase"))
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

    async fn list_resource_summaries<E>(
        &self,
        query_kind: RemoteListQuery<'_>,
        account: Option<&domain::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceSummaryView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = query_builder::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                query_builder::list_resources_query(kind, page, per_page, account)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                query_builder::list_all_resources_query(page, per_page, account).map_err(E::from)?
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_by_kind.nodes
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

    async fn list_resource_identities<E>(
        &self,
        query_kind: RemoteListQuery<'_>,
        account: Option<&domain::ResourceManifestAccount>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ResourceIdentityView>, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) = query_builder::graphql_page_params(offset, limit);

        let query = match query_kind {
            RemoteListQuery::ByKind(kind) => {
                query_builder::list_resource_identities_query(kind, page, per_page, account)
                    .map_err(E::from)?
            }
            RemoteListQuery::All => {
                query_builder::list_all_resource_identities_query(page, per_page, account)
                    .map_err(E::from)?
            }
        };

        let nodes = match query_kind {
            RemoteListQuery::ByKind(_) => {
                let response: fragments::ListIdentitiesByKindQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_identities_by_kind.nodes
            }
            RemoteListQuery::All => {
                let response: fragments::ListAllIdentitiesQueryDataFragment =
                    self.execute_graphql(&query).await?;
                response.resources.list_all_identities.nodes
            }
        };

        Ok(nodes.into_iter().map(Into::into).collect())
    }

    async fn search_resource_identities<E>(
        &self,
        request: &SearchResourceIdentitiesRequest,
    ) -> Result<SearchResourceIdentitiesResponse, E>
    where
        E: From<GraphqlHttpRequestError> + From<InternalError>,
    {
        let (page, per_page) =
            query_builder::graphql_page_params(request.pagination.offset, request.pagination.limit);
        let query = query_builder::search_resource_identities_query(request, page, per_page)
            .map_err(E::from)?;
        let response: fragments::SearchIdentitiesQueryDataFragment =
            self.execute_graphql(&query).await?;
        let connection = response.resources.search_identities;

        Ok(SearchResourceIdentitiesResponse {
            items: connection.nodes.into_iter().map(Into::into).collect(),
            total_count: connection.total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        let response: fragments::SupportedKindsQueryDataFragment = self
            .execute_graphql(query_builder::SUPPORTED_KINDS_QUERY)
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
                                data_type: query_builder::parse_enum(
                                    &column.data_type,
                                    "list column data type",
                                )?,
                                visibility: query_builder::parse_enum(
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
        let query = query_builder::summary_query(request.account.as_ref())
            .map_err(ResourcesSummaryError::Internal)?;

        let response: fragments::SummaryQueryDataFragment = self.execute_graphql(&query).await?;

        Ok(ResourcesSummary {
            resource_counts: response
                .resources
                .summary
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
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<domain::ResourceView, GetResourceError> {
        let query = query_builder::get_resource_query(&selector, spec_view_mode)?;

        let response: fragments::GetResourceQueryDataFragment =
            self.execute_graphql(&query).await?;

        let Some(resource) = response.resources.resource else {
            return Err(error_mapper::not_found_error(&selector));
        };

        resource.try_into().map_err(GetResourceError::Internal)
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<domain::ResourceView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let query = query_builder::get_resources_query(&selector, spec_view_mode)?;

        let response: fragments::BatchGetResourcesQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.resources;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| {
                Ok(BatchResourceSuccess {
                    request_index: success.request_index,
                    item: success
                        .resource
                        .try_into()
                        .map_err(BatchResourceError::Internal)?,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "resource")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_identity(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let query = query_builder::get_resource_identity_query(&selector)?;

        let response: fragments::GetResourceIdentityQueryDataFragment =
            self.execute_graphql(&query).await?;

        let Some(identity) = response.resources.resource_identity else {
            return Err(error_mapper::not_found_error(&selector));
        };

        Ok(identity.into())
    }

    async fn get_identities(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<ResourceIdentityView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let query = query_builder::get_resource_identities_query(&selector)?;

        let response: fragments::BatchGetResourceIdentitiesQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.resource_identities;

        let successes = batch_result
            .identities
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: success.identity.into(),
            })
            .collect();

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "identity")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn render_manifest(
        &self,
        selector: ResourceSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let query = query_builder::render_manifest_query(&selector, format, spec_view_mode)?;

        let response: fragments::RenderManifestQueryDataFragment = self
            .execute_graphql(&query)
            .await
            .map_err(|error| error_mapper::map_render_manifest_remote_error(&selector, error))?;

        let rendered = response.resources.render_manifest;

        Ok(RenderResourceManifestResult {
            manifest: rendered.manifest,
            format: rendered.format.into(),
        })
    }

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let query = query_builder::render_manifests_query(&selector, format, spec_view_mode)?;

        let response: fragments::BatchRenderManifestsQueryDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.render_manifests;

        let successes = batch_result
            .manifests
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: RenderResourceManifestResult {
                    manifest: success.manifest.manifest,
                    format: success.manifest.format.into(),
                },
            })
            .collect();

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "manifest")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListResourcesError> {
        self.list_resource_summaries::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        self.list_resource_identities::<ListResourcesError>(
            RemoteListQuery::ByKind(&request.kind),
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn search_identities(
        &self,
        request: SearchResourceIdentitiesRequest,
    ) -> Result<SearchResourceIdentitiesResponse, ListResourcesError> {
        self.search_resource_identities::<ListResourcesError>(&request)
            .await
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListAllResourcesError> {
        self.list_resource_summaries::<ListAllResourcesError>(
            RemoteListQuery::All,
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError> {
        self.list_resource_identities::<ListAllResourcesError>(
            RemoteListQuery::All,
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .await
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        let query = query_builder::apply_manifest_query(&request, true)?;
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
        let query = query_builder::apply_manifest_query(&request, false)?;
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
        selector: ResourceSelector,
    ) -> Result<domain::ResourceUID, DeleteResourceError> {
        let query = query_builder::delete_resource_query(&selector)?;

        let response: fragments::DeleteMutationDataFragment = self
            .execute_graphql(&query)
            .await
            .map_err(|error| error_mapper::map_delete_remote_error(&selector, error))?;

        Ok(response.resources.delete.resource_id)
    }

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<domain::ResourceUID, ResourceLookupProblem>, BatchResourceError>
    {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let query = query_builder::delete_resources_query(&selector)?;

        let response: fragments::DeleteManyMutationDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.delete_many;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: success.resource_id,
            })
            .collect();

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "delete")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum RemoteListQuery<'a> {
    ByKind(&'a str),
    All,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
