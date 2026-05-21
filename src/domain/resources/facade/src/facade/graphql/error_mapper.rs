// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::GraphqlHttpRequestError;
use internal_error::InternalError;
use kamu_resources as domain;
use kamu_resources::{ResourceAPIVersionMismatchError, ResourceUIDNotFoundError};

use super::fragments;
use crate::{
    BatchResourceError,
    BatchResourceProblem,
    DeleteResourceError,
    GetResourceError,
    RenderResourceManifestError,
    ResourceBatchSelector,
    ResourceKindMismatchError,
    ResourceLookupProblem,
    ResourceRef,
    ResourceSelector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn not_found_error(selector: &ResourceSelector) -> GetResourceError {
    match &selector.resource_ref {
        ResourceRef::ById(uid) => GetResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(ResourceUIDNotFoundError(*uid)),
        ),
        ResourceRef::ByName(name) => GetResourceError::LookupProblem(
            ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
                kind: selector.kind.clone(),
                name: name.clone(),
            }),
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn collect_batch_problems(
    selector: &ResourceBatchSelector,
    problems: Vec<fragments::BatchResourceProblemFragment>,
    context: &str,
) -> Result<Vec<BatchResourceProblem<ResourceLookupProblem>>, BatchResourceError> {
    problems
        .into_iter()
        .map(|problem| {
            let resource_ref = selector
                .resource_refs
                .get(problem.request_index)
                .ok_or_else(|| {
                    BatchResourceError::Internal(InternalError::new(format!(
                        "Remote {context} problem index {} is out of bounds",
                        problem.request_index
                    )))
                })?;
            let request_index = problem.request_index;
            let error = batch_resource_problem_error(
                &problem,
                &selector.kind,
                resource_ref,
                selector.api_version.as_deref(),
            )?;
            Ok(BatchResourceProblem {
                request_index,
                error,
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn batch_resource_problem_error(
    problem: &fragments::BatchResourceProblemFragment,
    kind: &str,
    resource_ref: &ResourceRef,
    expected_api_version: Option<&str>,
) -> Result<ResourceLookupProblem, BatchResourceError> {
    use fragments::BatchResourceProblemCodeFragment as Code;
    Ok(match &problem.code {
        Code::UidNotFound => match resource_ref {
            ResourceRef::ById(uid) => {
                ResourceLookupProblem::UIDNotFound(ResourceUIDNotFoundError(*uid))
            }
            ResourceRef::ByName(_) => return Err(malformed_remote_problem(problem)),
        },
        Code::NameNotFound => match resource_ref {
            ResourceRef::ByName(name) => {
                ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
                    kind: kind.to_string(),
                    name: name.clone(),
                })
            }
            ResourceRef::ById(_) => return Err(malformed_remote_problem(problem)),
        },
        Code::ApiVersionMismatch => {
            let Some(expected_api_version) = expected_api_version else {
                return Err(malformed_remote_problem(problem));
            };
            let Some(actual_api_version) = parse_actual_api_version(&problem.message) else {
                return Err(malformed_remote_problem(problem));
            };

            ResourceLookupProblem::ApiVersionMismatch(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.to_string(),
                actual_api_version,
            })
        }
        Code::KindMismatch => match resource_ref {
            ResourceRef::ById(uid) => {
                let Some(actual_kind) = parse_actual_kind(*uid, kind, &problem.message) else {
                    return Err(malformed_remote_problem(problem));
                };

                ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                    uid: *uid,
                    expected_kind: kind.to_string(),
                    actual_kind,
                })
            }
            ResourceRef::ByName(_) => return Err(malformed_remote_problem(problem)),
        },
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn malformed_remote_problem(
    problem: &fragments::BatchResourceProblemFragment,
) -> BatchResourceError {
    BatchResourceError::Internal(InternalError::new(format!(
        "Malformed remote resource problem: code={:?}, message={}",
        problem.code, problem.message
    )))
}

fn parse_actual_api_version(message: &str) -> Option<String> {
    let prefix = "Resource api version mismatch: expected '";
    let (_, actual) = message.strip_prefix(prefix)?.split_once("', actual '")?;
    let actual_end = actual.find('\'')?;
    Some(actual[..actual_end].to_string())
}

fn parse_actual_kind(
    uid: domain::ResourceUID,
    expected_kind: &str,
    message: &str,
) -> Option<String> {
    let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
    let mismatch_suffix = format!("', expected '{expected_kind}'");
    let actual_kind_start = message.find(&mismatch_prefix)? + mismatch_prefix.len();
    let actual_kind_end = message[actual_kind_start..].find(&mismatch_suffix)?;
    Some(message[actual_kind_start..actual_kind_start + actual_kind_end].to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_delete_remote_error(
    selector: &ResourceSelector,
    error: GraphqlHttpRequestError,
) -> DeleteResourceError {
    match error {
        GraphqlHttpRequestError::Graphql {
            endpoint_url,
            message,
        } => map_delete_graphql_error(selector, &message).unwrap_or_else(|| {
            DeleteResourceError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            })
        }),
        GraphqlHttpRequestError::Internal(error) => DeleteResourceError::Internal(error),
        other => DeleteResourceError::RemoteRequest(other),
    }
}

fn map_delete_graphql_error(
    selector: &ResourceSelector,
    message: &str,
) -> Option<DeleteResourceError> {
    match &selector.resource_ref {
        ResourceRef::ById(uid) => {
            let not_found = domain::ResourceUIDNotFoundError(*uid);
            if message.contains(&not_found.to_string()) {
                return Some(DeleteResourceError::LookupProblem(
                    ResourceLookupProblem::UIDNotFound(not_found),
                ));
            }

            let mismatch_prefix = format!("Resource uid {uid} refers to kind '");
            let mismatch_suffix = format!("', expected '{}'", selector.kind);
            if let Some(actual_kind_start) = message.find(&mismatch_prefix) {
                let actual_kind_start = actual_kind_start + mismatch_prefix.len();
                if let Some(actual_kind_end) = message[actual_kind_start..].find(&mismatch_suffix) {
                    return Some(DeleteResourceError::LookupProblem(
                        ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                            uid: *uid,
                            expected_kind: selector.kind.clone(),
                            actual_kind: message
                                [actual_kind_start..actual_kind_start + actual_kind_end]
                                .to_string(),
                        }),
                    ));
                }
            }

            None
        }
        ResourceRef::ByName(name) => {
            let not_found = domain::ResourceNameNotFoundError {
                kind: selector.kind.clone(),
                name: name.clone(),
            };
            message
                .contains(&not_found.to_string())
                .then_some(DeleteResourceError::LookupProblem(
                    ResourceLookupProblem::NameNotFound(not_found),
                ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_render_manifest_remote_error(
    selector: &ResourceSelector,
    error: GraphqlHttpRequestError,
) -> RenderResourceManifestError {
    match error {
        GraphqlHttpRequestError::Graphql {
            endpoint_url,
            message,
        } => map_render_manifest_graphql_error(selector, &message).unwrap_or_else(|| {
            RenderResourceManifestError::RemoteRequest(GraphqlHttpRequestError::Graphql {
                endpoint_url,
                message,
            })
        }),
        GraphqlHttpRequestError::Internal(error) => RenderResourceManifestError::Internal(error),
        other => RenderResourceManifestError::RemoteRequest(other),
    }
}

fn map_render_manifest_graphql_error(
    selector: &ResourceSelector,
    message: &str,
) -> Option<RenderResourceManifestError> {
    match &selector.resource_ref {
        ResourceRef::ById(uid) => {
            let error = domain::ResourceUIDNotFoundError(*uid);
            message.contains(&error.to_string()).then_some(
                RenderResourceManifestError::LookupProblem(ResourceLookupProblem::UIDNotFound(
                    error,
                )),
            )
        }
        ResourceRef::ByName(name) => {
            let error = domain::ResourceNameNotFoundError {
                kind: selector.kind.clone(),
                name: name.clone(),
            };
            message.contains(&error.to_string()).then_some(
                RenderResourceManifestError::LookupProblem(ResourceLookupProblem::NameNotFound(
                    error,
                )),
            )
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
