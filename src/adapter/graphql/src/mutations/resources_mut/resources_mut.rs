// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{ResourceApplyOutcome, ResourceApplyParseManifestProblem};
use crate::prelude::*;
use crate::queries::{
    BatchResourceProblem,
    ResourceAccountSelectorInput,
    ResourceBadAccountProblem,
    ResourceBatchSelectorInput,
    ResourceManifestFormat,
    ResourceSelectorInput,
    ResourceSelectorProblem,
    ResourceSelectorProblemResult,
    ResourceUnsupportedDescriptorProblem,
    map_bad_account_problem,
    map_resolve_manifest_account_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourcesMut;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl ResourcesMut {
    #[tracing::instrument(level = "info", name = ResourcesMut_apply_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn apply_manifest(
        &self,
        ctx: &Context<'_>,
        manifest: String,
        format: ResourceManifestFormat,
        dry_run: Option<bool>,
    ) -> Result<ResourceApplyOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let request = kamu_resources_facade::ApplyManifestRequest {
            format: format.into(),
            manifest,
        };

        let outcome_result = if dry_run.unwrap_or(false) {
            resource_facade
                .plan_apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        } else {
            resource_facade
                .apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        };

        match outcome_result {
            Ok(outcome) => Ok(outcome),
            Err(err) => map_apply_resource_error(err),
        }
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<ResourceDeleteOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let ResourceSelectorInput {
            kind,
            resource_ref,
            account,
        } = selector;
        let kind = kind.into_resource_type();

        match resource_facade
            .delete(kamu_resources_facade::ResourceSelector {
                kind,
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                resource_ref: resource_ref.into(),
            })
            .await
        {
            Ok(resource_id) => Ok(ResourceDeleteOutcome::Success(ResourceDeleteSuccess {
                resource_id: resource_id.into(),
            })),
            Err(kamu_resources_facade::DeleteResourceError::LookupProblem(problem)) => {
                Ok(ResourceDeleteOutcome::Problem(problem.into()))
            }
            Err(kamu_resources_facade::DeleteResourceError::UnsupportedDescriptor(e)) => {
                Ok(ResourceDeleteOutcome::Problem(e.into()))
            }
            Err(kamu_resources_facade::DeleteResourceError::BadAccount(e)) => Ok(
                ResourceDeleteOutcome::Problem(ResourceSelectorProblemResult {
                    problem: ResourceSelectorProblem::BadAccount(map_bad_account_problem(e)?),
                }),
            ),
            Err(error) => Err(map_delete_resource_error(error)),
        }
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete_many, skip_all, fields(selector_count = selector.resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete_many(
        &self,
        ctx: &Context<'_>,
        selector: ResourceBatchSelectorInput,
    ) -> Result<ResourceDeleteManyOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let ResourceBatchSelectorInput {
            kind,
            resource_refs,
            account,
        } = selector;
        let kind = kind.into_resource_type();

        match resource_facade
            .delete_many(kamu_resources_facade::ResourceBatchSelector {
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                kind,
                resource_refs: resource_refs.into_iter().map(Into::into).collect(),
            })
            .await
        {
            Ok(response) => Ok(ResourceDeleteManyOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedDescriptor(e)) => {
                Ok(ResourceDeleteManyOutcome::UnsupportedDescriptor(e.into()))
            }
            Err(kamu_resources_facade::BatchResourceError::BadAccount(e)) => Ok(
                ResourceDeleteManyOutcome::BadAccount(map_bad_account_problem(e)?),
            ),
            Err(e) => Err(map_batch_delete_resource_error(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceDeleteOutcome {
    Success(ResourceDeleteSuccess),
    Problem(ResourceSelectorProblemResult),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteSuccess {
    pub resource_id: ResourceID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceDeleteManyOutcome {
    Success(ResourceDeleteManyResult),
    UnsupportedDescriptor(ResourceUnsupportedDescriptorProblem),
    BadAccount(ResourceBadAccountProblem),
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteManyResult {
    pub resources: Vec<ResourceDeleteManySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

type BatchDeleteResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceID,
    kamu_resources_facade::ResourceLookupProblem,
>;

impl From<BatchDeleteResourcesResponse> for ResourceDeleteManyResult {
    fn from(value: BatchDeleteResourcesResponse) -> Self {
        Self {
            resources: value
                .successes
                .into_iter()
                .map(|success| ResourceDeleteManySuccess {
                    request_index: success.request_index,
                    resource_id: success.item.into(),
                })
                .collect(),
            problems: value.problems.into_iter().map(Into::into).collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteManySuccess {
    pub request_index: usize,
    pub resource_id: ResourceID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_apply_resource_error(
    error: kamu_resources_facade::ApplyManifestError,
) -> Result<ResourceApplyOutcome, GqlError> {
    use kamu_resources_facade::ApplyManifestError as E;

    match error {
        E::ParseManifest(e) => Ok(ResourceApplyOutcome::ParseManifest(
            ResourceApplyParseManifestProblem {
                message: e.to_string(),
            },
        )),
        E::UnsupportedDescriptor(e) => Ok(ResourceApplyOutcome::UnsupportedDescriptor(e.into())),
        E::BadAccount(e) => map_bad_account_problem(e).map(ResourceApplyOutcome::BadAccount),
        E::InvalidHeaders(e) => Ok(ResourceApplyOutcome::InvalidHeader(e.into())),
        E::InvalidSpec(e) => Ok(ResourceApplyOutcome::InvalidSpec(e.into())),
        E::IDNotFound(error) => Err(GqlError::gql(error.to_string())),
        E::TypeMismatch(error) => Err(GqlError::gql(error.to_string())),
        E::ConcurrentModification(error) => {
            tracing::error!(error = ?error, "Resource apply_manifest concurrent modification");
            Err(GqlError::gql("Resource was modified concurrently"))
        }
        E::RemoteRequest(error) => Err(error.int_err().into()),
        E::Internal(error) => Err(error.into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_delete_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_delete_resource_error(error: kamu_resources_facade::DeleteResourceError) -> GqlError {
    use kamu_resources_facade::DeleteResourceError as E;

    match error {
        E::UnsupportedDescriptor(_) => {
            unreachable!("UnsupportedDescriptor is handled as a union arm")
        }
        E::BadAccount(_) => unreachable!("BadAccount is handled as a union arm"),
        E::LookupProblem(_) => unreachable!("LookupProblem is handled as a union arm"),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
