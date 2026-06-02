// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{ResourceApplyError, ResourceApplyErrorCode, ResourceApplyOutcome};
use crate::prelude::*;
use crate::queries::{
    BatchResourceProblem,
    ResourceAccountSelectorInput,
    ResourceBatchSelectorInput,
    ResourceKind,
    ResourceManifestFormat,
    ResourceSelectorInput,
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
            api_version,
            resource_ref,
            account,
        } = selector;
        let kind = kind.into_resource_type();

        match resource_facade
            .delete(kamu_resources_facade::ResourceSelector {
                kind: kind.clone(),
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                api_version,
                resource_ref: resource_ref.into(),
            })
            .await
        {
            Ok(resource_id) => Ok(ResourceDeleteOutcome::Success(ResourceDeleteSuccess {
                resource_id: resource_id.into(),
                kind: ResourceKind::new(kind).into(),
            })),
            Err(kamu_resources_facade::DeleteResourceError::LookupProblem(problem)) => {
                Ok(ResourceDeleteOutcome::from_lookup_problem(problem))
            }
            Err(error) => Err(map_delete_resource_error(error)),
        }
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete_many, skip_all, fields(selector_count = selector.resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete_many(
        &self,
        ctx: &Context<'_>,
        selector: ResourceBatchSelectorInput,
    ) -> Result<ResourceDeleteManyResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let ResourceBatchSelectorInput {
            kind,
            api_version,
            resource_refs,
            account,
        } = selector;
        let kind = kind.into_resource_type();

        resource_facade
            .delete_many(kamu_resources_facade::ResourceBatchSelector {
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                kind,
                api_version,
                resource_refs: resource_refs.into_iter().map(Into::into).collect(),
            })
            .await
            .map(Into::into)
            .map_err(map_batch_delete_resource_error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceDeleteOutcome {
    Success(ResourceDeleteSuccess),
    UidNotFound(ResourceUIDNotFoundProblem),
    NameNotFound(ResourceNameNotFoundProblem),
    ApiVersionMismatch(ResourceApiVersionMismatchProblem),
    KindMismatch(ResourceKindMismatchProblem),
}

impl ResourceDeleteOutcome {
    fn from_lookup_problem(
        problem: kamu_resources_facade::ResourceLookupProblem,
    ) -> ResourceDeleteOutcome {
        use kamu_resources_facade::ResourceLookupProblem as P;
        match problem {
            P::UIDNotFound(e) => Self::UidNotFound(ResourceUIDNotFoundProblem {
                uid: e.0.into(),
                message: e.to_string(),
            }),
            P::NameNotFound(e) => Self::NameNotFound(ResourceNameNotFoundProblem {
                kind: e.kind.clone(),
                name: e.name.clone(),
                message: e.to_string(),
            }),
            P::ApiVersionMismatch(e) => {
                Self::ApiVersionMismatch(ResourceApiVersionMismatchProblem {
                    expected_api_version: e.expected_api_version.clone(),
                    actual_api_version: e.actual_api_version.clone(),
                    message: e.to_string(),
                })
            }
            P::KindMismatch(e) => Self::KindMismatch(ResourceKindMismatchProblem {
                uid: e.uid.into(),
                expected_kind: e.expected_kind.clone(),
                actual_kind: e.actual_kind.clone(),
                message: e.to_string(),
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteSuccess {
    pub resource_id: ResourceID,
    pub kind: Option<ResourceKind>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceUIDNotFoundProblem {
    pub uid: ResourceID,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceNameNotFoundProblem {
    pub kind: String,
    pub name: String,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApiVersionMismatchProblem {
    pub expected_api_version: String,
    pub actual_api_version: String,
    pub message: String,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceKindMismatchProblem {
    pub uid: ResourceID,
    pub expected_kind: String,
    pub actual_kind: String,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteManyResult {
    pub resources: Vec<ResourceDeleteManySuccess>,
    pub problems: Vec<BatchResourceProblem>,
}

type BatchDeleteResourcesResponse = kamu_resources_facade::BatchResourceResponse<
    kamu_resources::ResourceUID,
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

    let apply_error = |code, message: String| {
        Ok(ResourceApplyOutcome::Error(ResourceApplyError {
            code,
            message,
        }))
    };

    match error {
        E::ParseManifest(e) => apply_error(ResourceApplyErrorCode::ParseManifest, e.to_string()),
        E::UnsupportedDescriptor(e) => {
            apply_error(ResourceApplyErrorCode::UnsupportedDescriptor, e.to_string())
        }
        E::BadAccount(e) => apply_error(ResourceApplyErrorCode::BadAccount, e.to_string()),
        E::InvalidMetadata(e) => {
            apply_error(ResourceApplyErrorCode::InvalidMetadata, e.to_string())
        }
        E::InvalidSpec(e) => apply_error(ResourceApplyErrorCode::InvalidSpec, e.to_string()),
        E::UIDNotFound(error) => Err(GqlError::gql(error.to_string())),
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
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::LookupProblem(_) => unreachable!("LookupProblem is handled as a union arm"),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
