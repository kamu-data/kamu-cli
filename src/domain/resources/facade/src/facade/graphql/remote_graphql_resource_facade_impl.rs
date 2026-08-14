// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::GraphqlHttpClient;
use kamu_resources as domain;
use kamu_resources::ResourceRef;
use url::Url;

use crate::facade::graphql::{cynic_api, outcome_mapper};
use crate::{
    ApplyManifestBatchRequest,
    ApplyManifestBatchResponse,
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    BatchResourceResponse,
    DeleteResourceError,
    GetResourceError,
    ListResourcesError,
    ListSupportedResourceTypesError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourceHandlesRequest,
    SearchResourceHandlesResponse,
    SearchResourcesRequest,
    SearchResourcesResponse,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<domain::ResourceTypeDescriptor>, ListSupportedResourceTypesError> {
        use cynic_api::operations::supported_resource_types as Operation;

        let response: Operation::SupportedResourceTypesQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation())
            .await?;

        Ok(response
            .resources
            .supported_resource_types
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<domain::ResourcesSummary, ResourcesSummaryError> {
        use cynic_api::operations::summary as Operation;

        let variables = Operation::SummaryVariables::new(&request);

        let response: Operation::SummaryQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_summary_outcome(response.resources.summary)
    }

    async fn get(
        &self,
        resource_ref: ResourceRef,
        spec_view_mode: SpecViewMode,
    ) -> Result<domain::Resource, GetResourceError> {
        use cynic_api::operations::get_resource as Operation;

        let variables = Operation::ResourceRefVariables::new(&resource_ref, spec_view_mode);

        let response: Operation::GetResourceQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_get_resource_outcome(response.resources.resource)
    }

    async fn get_many(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<domain::Resource, ResourceLookupProblem>, BatchResourceError>
    {
        use cynic_api::operations::get_resources as Operation;

        let variables = Operation::ResourceRefsVariables::new(&resource_refs, spec_view_mode);

        let response: Operation::GetResourcesQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_batch_get_resources_outcome(
            response.resources.resources,
            &resource_refs,
        )
    }

    async fn get_handle(
        &self,
        resource_ref: ResourceRef,
    ) -> Result<domain::ResourceHandle, GetResourceError> {
        use cynic_api::operations::handle as Operation;

        let variables = Operation::ResourceHandleRefVariables::new(&resource_ref);

        let response: Operation::GetResourceHandleQuery = self
            .graphql_client
            .execute_operation(Operation::build_handle_operation(variables))
            .await?;

        outcome_mapper::map_get_handle_outcome(response.resources.resource_handle)
    }

    async fn get_handles(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<
        BatchResourceResponse<domain::ResourceHandle, ResourceLookupProblem>,
        BatchResourceError,
    > {
        use cynic_api::operations::handle as Operation;

        let variables = Operation::ResourceHandleRefsVariables::new(&resource_refs);

        let response: Operation::GetResourceHandlesQuery = self
            .graphql_client
            .execute_operation(Operation::build_handles_operation(variables))
            .await?;

        outcome_mapper::map_batch_get_handles_outcome(
            response.resources.resource_handles,
            &resource_refs,
        )
    }

    async fn render_manifest(
        &self,
        resource_ref: ResourceRef,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        use cynic_api::operations::render_manifest as Operation;

        let variables =
            Operation::RenderResourceManifestVariables::new(&resource_ref, format, spec_view_mode);

        let response: Operation::RenderManifestQuery = self
            .graphql_client
            .execute_operation(Operation::build_manifest_operation(variables))
            .await?;

        outcome_mapper::map_render_manifest_outcome(response.resources.render_manifest)
    }

    async fn render_manifests(
        &self,
        resource_refs: Vec<ResourceRef>,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        use cynic_api::operations::render_manifest as Operation;

        let variables = Operation::RenderResourceManifestsVariables::new(
            &resource_refs,
            format,
            spec_view_mode,
        );

        let response: Operation::RenderManifestsQuery = self
            .graphql_client
            .execute_operation(Operation::build_manifests_operation(variables))
            .await?;

        outcome_mapper::map_batch_render_manifests_outcome(
            response.resources.render_manifests,
            &resource_refs,
        )
    }

    async fn search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourcesResponse, ListResourcesError> {
        use cynic_api::operations::search_summaries as Operation;

        let variables =
            Operation::SearchVariables::new(&request).map_err(ListResourcesError::Internal)?;

        let response: Operation::SearchQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_search_outcome(response.resources.search)
    }

    async fn search_handles(
        &self,
        request: SearchResourceHandlesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
        use cynic_api::operations::search as SearchOperation;

        let variables = SearchOperation::SearchHandlesVariables::new(&request)
            .map_err(ListResourcesError::Internal)?;

        let response: SearchOperation::SearchHandlesQuery = self
            .graphql_client
            .execute_operation(SearchOperation::build_operation(variables))
            .await?;

        outcome_mapper::map_search_handles_outcome(response.resources.search_handles)
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        use cynic_api::operations::apply as Operation;

        let variables = Operation::ApplyManifestVariables::new(request, true);

        let response: Operation::ApplyManifestMutation = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        response
            .resources
            .apply_manifest
            .try_into_planning_decision()
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
        use cynic_api::operations::apply as Operation;

        let variables = Operation::ApplyManifestVariables::new(request, false);

        let response: Operation::ApplyManifestMutation = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        response
            .resources
            .apply_manifest
            .try_into_application_decision()
    }

    async fn plan_apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<domain::ApplyManifestPlanningDecision>, BatchResourceError>
    {
        use cynic_api::operations::apply_batch as Operation;

        let variables = Operation::ApplyManifestsVariables::new(request, true);

        match self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await
        {
            Ok(response) => outcome_mapper::map_batch_apply_manifests_planning_outcome(
                response.resources.apply_manifests,
            ),
            Err(error) => outcome_mapper::map_batch_apply_manifests_planning_rollback(error),
        }
    }

    async fn apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<
        ApplyManifestBatchResponse<domain::ApplyManifestApplicationDecision>,
        BatchResourceError,
    > {
        use cynic_api::operations::apply_batch as Operation;

        let variables = Operation::ApplyManifestsVariables::new(request, false);

        match self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await
        {
            Ok(response) => outcome_mapper::map_batch_apply_manifests_application_outcome(
                response.resources.apply_manifests,
            ),
            Err(error) => outcome_mapper::map_batch_apply_manifests_application_rollback(error),
        }
    }

    async fn delete(
        &self,
        resource_ref: ResourceRef,
    ) -> Result<domain::ResourceID, DeleteResourceError> {
        use cynic_api::operations::delete as Operation;

        let variables = Operation::DeleteVariables {
            resource_ref: (&resource_ref).into(),
        };

        let response: Operation::DeleteMutation = self
            .graphql_client
            .execute_operation(Operation::build_delete_operation(variables))
            .await?;

        outcome_mapper::map_delete_outcome(response.resources.delete)
    }

    async fn delete_many(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<domain::ResourceID, ResourceLookupProblem>, BatchResourceError>
    {
        use cynic_api::operations::delete as Operation;

        let variables = Operation::DeleteManyVariables {
            resource_refs: cynic_api::inputs::resource_ref_inputs(&resource_refs),
        };

        let response: Operation::DeleteManyMutation = self
            .graphql_client
            .execute_operation(Operation::build_delete_many_operation(variables))
            .await?;

        outcome_mapper::map_batch_delete_many_outcome(
            response.resources.delete_many,
            &resource_refs,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
