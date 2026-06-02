// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources as domain;

use crate::facade::graphql::cynic_api;
use crate::{
    BatchResourceError,
    BatchResourceProblem,
    BatchResourceSuccess,
    DeleteResourceError,
    GetResourceError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceKindMismatchError,
    ResourceLookupProblem,
    ResourceRef,
    ResourceSelector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn collect_batch_successes<S, T, F>(
    request_len: usize,
    items: Vec<S>,
    context: &str,
    map_item: F,
) -> Result<Vec<BatchResourceSuccess<T>>, BatchResourceError>
where
    F: Fn(S) -> Result<(i32, T), BatchResourceError>,
{
    let mut seen = std::collections::BTreeSet::new();
    items
        .into_iter()
        .map(|s| {
            let (raw_index, item) = map_item(s)?;
            let request_index = usize::try_from(raw_index).map_err(|_| {
                BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} success index {raw_index} cannot be converted to usize",
                )))
            })?;
            if request_index >= request_len {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} success index {request_index} is out of bounds",
                ))));
            }
            if !seen.insert(request_index) {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} returned duplicate success index {request_index}",
                ))));
            }
            Ok(BatchResourceSuccess {
                request_index,
                item,
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn not_found_error(selector: &ResourceSelector) -> GetResourceError {
    match &selector.resource_ref {
        ResourceRef::ById(uid) => GetResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(*uid)),
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
    problems: Vec<impl BatchResourceProblemLike>,
    context: &str,
) -> Result<Vec<BatchResourceProblem<ResourceLookupProblem>>, BatchResourceError> {
    problems
        .into_iter()
        .map(|problem| {
            let request_index = problem.request_index()?;
            let resource_ref = selector.resource_refs.get(request_index).ok_or_else(|| {
                BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} problem index {request_index} is out of bounds",
                )))
            })?;
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

pub(super) fn validate_batch_response_indexes<T, E>(
    successes: &[BatchResourceSuccess<T>],
    problems: &[BatchResourceProblem<E>],
    request_len: usize,
    context: &str,
) -> Result<(), BatchResourceError> {
    let mut seen: std::collections::BTreeMap<usize, &str> = std::collections::BTreeMap::new();

    for s in successes {
        if s.request_index >= request_len {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} success index {} is out of bounds",
                s.request_index,
            ))));
        }
        if seen.insert(s.request_index, "success").is_some() {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} returned duplicate result index {}",
                s.request_index,
            ))));
        }
    }

    for p in problems {
        if p.request_index >= request_len {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} problem index {} is out of bounds",
                p.request_index,
            ))));
        }
        if let Some(previous) = seen.insert(p.request_index, "problem") {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} returned both {previous} and problem for index {}",
                p.request_index,
            ))));
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn batch_resource_problem_error(
    problem: &impl BatchResourceProblemLike,
    kind: &str,
    resource_ref: &ResourceRef,
    expected_api_version: Option<&str>,
) -> Result<ResourceLookupProblem, BatchResourceError> {
    Ok(match problem.code() {
        BatchResourceProblemCode::UidNotFound => match resource_ref {
            ResourceRef::ById(uid) => {
                ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(*uid))
            }
            ResourceRef::ByName(_) => return Err(malformed_remote_problem(problem)),
        },
        BatchResourceProblemCode::NameNotFound => match resource_ref {
            ResourceRef::ByName(name) => {
                ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
                    kind: kind.to_string(),
                    name: name.clone(),
                })
            }
            ResourceRef::ById(_) => return Err(malformed_remote_problem(problem)),
        },
        BatchResourceProblemCode::ApiVersionMismatch => {
            let Some(expected_api_version) = expected_api_version else {
                return Err(malformed_remote_problem(problem));
            };
            let Some(actual_api_version) = problem.actual_api_version() else {
                return Err(malformed_remote_problem(problem));
            };

            ResourceLookupProblem::ApiVersionMismatch(domain::ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.to_string(),
                actual_api_version: actual_api_version.to_string(),
            })
        }
        BatchResourceProblemCode::KindMismatch => match resource_ref {
            ResourceRef::ById(uid) => {
                let Some(actual_kind) = problem.actual_kind() else {
                    return Err(malformed_remote_problem(problem));
                };

                ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                    uid: *uid,
                    expected_kind: kind.to_string(),
                    actual_kind: actual_kind.to_string(),
                })
            }
            ResourceRef::ByName(_) => return Err(malformed_remote_problem(problem)),
        },
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) trait BatchResourceProblemLike {
    fn request_index(&self) -> Result<usize, BatchResourceError>;
    fn code(&self) -> BatchResourceProblemCode;
    fn code_debug(&self) -> String;
    fn message(&self) -> &str;
    fn actual_api_version(&self) -> Option<&str>;
    fn actual_kind(&self) -> Option<&str>;
}

#[derive(Debug, Clone, Copy)]
pub(super) enum BatchResourceProblemCode {
    UidNotFound,
    NameNotFound,
    ApiVersionMismatch,
    KindMismatch,
}

impl BatchResourceProblemLike for cynic_api::fragments::BatchResourceProblem {
    fn request_index(&self) -> Result<usize, BatchResourceError> {
        usize::try_from(self.request_index).map_err(|_| {
            BatchResourceError::Internal(InternalError::new(format!(
                "Remote resource problem index {} cannot be converted to usize",
                self.request_index
            )))
        })
    }

    fn code(&self) -> BatchResourceProblemCode {
        match self.code {
            cynic_api::fragments::BatchResourceProblemCode::UidNotFound => {
                BatchResourceProblemCode::UidNotFound
            }
            cynic_api::fragments::BatchResourceProblemCode::NameNotFound => {
                BatchResourceProblemCode::NameNotFound
            }
            cynic_api::fragments::BatchResourceProblemCode::ApiVersionMismatch => {
                BatchResourceProblemCode::ApiVersionMismatch
            }
            cynic_api::fragments::BatchResourceProblemCode::KindMismatch => {
                BatchResourceProblemCode::KindMismatch
            }
        }
    }

    fn code_debug(&self) -> String {
        format!("{:?}", self.code)
    }

    fn message(&self) -> &str {
        &self.message
    }

    fn actual_api_version(&self) -> Option<&str> {
        self.actual_api_version.as_deref()
    }

    fn actual_kind(&self) -> Option<&str> {
        self.actual_kind.as_deref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn malformed_remote_problem(problem: &impl BatchResourceProblemLike) -> BatchResourceError {
    BatchResourceError::Internal(InternalError::new(format!(
        "Malformed remote resource problem: code={}, message={}",
        problem.code_debug(),
        problem.message()
    )))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_delete_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteOutcome,
) -> Result<domain::ResourceUID, DeleteResourceError> {
    use cynic_api::operations::delete::ResourceDeleteOutcome as O;
    match outcome {
        O::ResourceDeleteSuccess(s) => Ok(s.resource_id),
        O::ResourceUIDNotFoundProblem(p) => Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(p.uid)),
        )),
        O::ResourceNameNotFoundProblem(p) => Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
                kind: p.kind,
                name: p.name,
            }),
        )),
        O::ResourceApiVersionMismatchProblem(p) => Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(domain::ResourceAPIVersionMismatchError {
                expected_api_version: p.expected_api_version,
                actual_api_version: p.actual_api_version,
            }),
        )),
        O::ResourceKindMismatchProblem(p) => Err(DeleteResourceError::LookupProblem(
            ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                uid: p.uid,
                expected_kind: p.expected_kind,
                actual_kind: p.actual_kind,
            }),
        )),
        O::Unknown => Err(DeleteResourceError::Internal(InternalError::new(
            "Remote delete returned an unrecognized ResourceDeleteOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_render_manifest_outcome(
    outcome: cynic_api::operations::render_manifest::ResourceRenderManifestOutcome,
) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
    use cynic_api::operations::render_manifest::ResourceRenderManifestOutcome as O;
    match outcome {
        O::ResourceRenderManifestResult(r) => Ok(r.into()),
        O::ResourceUIDNotFoundProblem(p) => Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(p.uid)),
        )),
        O::ResourceNameNotFoundProblem(p) => Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
                kind: p.kind,
                name: p.name,
            }),
        )),
        O::ResourceApiVersionMismatchProblem(p) => Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::ApiVersionMismatch(domain::ResourceAPIVersionMismatchError {
                expected_api_version: p.expected_api_version,
                actual_api_version: p.actual_api_version,
            }),
        )),
        O::ResourceKindMismatchProblem(p) => Err(RenderResourceManifestError::LookupProblem(
            ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
                uid: p.uid,
                expected_kind: p.expected_kind,
                actual_kind: p.actual_kind,
            }),
        )),
        O::Unknown => Err(RenderResourceManifestError::Internal(InternalError::new(
            "Remote render_manifest returned an unrecognized ResourceRenderManifestOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_resources::ResourceUID;

    use super::*;
    use crate::facade::graphql::cynic_api::fragments::{
        BatchResourceProblem,
        BatchResourceProblemCode,
    };

    fn make_selector(refs: Vec<ResourceRef>) -> ResourceBatchSelector {
        ResourceBatchSelector {
            account: None,
            kind: "TestKind".to_string(),
            api_version: None,
            resource_refs: refs,
        }
    }

    fn uid_ref() -> ResourceRef {
        let uid: ResourceUID =
            serde_json::from_str("\"00000000-0000-0000-0000-000000000000\"").unwrap();
        ResourceRef::ById(uid)
    }

    fn problem(index: i32, code: BatchResourceProblemCode) -> BatchResourceProblem {
        BatchResourceProblem {
            request_index: index,
            code,
            message: String::new(),
            actual_api_version: None,
            actual_kind: None,
        }
    }

    #[test]
    fn collect_batch_problems_negative_index_is_error() {
        let selector = make_selector(vec![uid_ref()]);
        let err = collect_batch_problems(
            &selector,
            vec![problem(-1, BatchResourceProblemCode::UidNotFound)],
            "test",
        )
        .unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for negative problem index"
        );
    }

    #[test]
    fn collect_batch_problems_out_of_bounds_index_is_error() {
        let selector = make_selector(vec![uid_ref()]);
        let err = collect_batch_problems(
            &selector,
            vec![problem(5, BatchResourceProblemCode::UidNotFound)],
            "test",
        )
        .unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for out-of-bounds problem index"
        );
    }

    fn collect<T>(
        request_len: usize,
        pairs: Vec<(i32, T)>,
    ) -> Result<Vec<BatchResourceSuccess<T>>, BatchResourceError> {
        collect_batch_successes(request_len, pairs, "test", Ok)
    }

    #[test]
    fn collect_batch_successes_happy_path() {
        let result = collect(3, vec![(0, "a"), (1, "b"), (2, "c")]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].request_index, 0);
        assert_eq!(result[1].request_index, 1);
        assert_eq!(result[2].request_index, 2);
    }

    #[test]
    fn collect_batch_successes_negative_index_is_error() {
        let err = collect(3, vec![(-1_i32, "x")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for negative index"
        );
    }

    #[test]
    fn collect_batch_successes_out_of_bounds_index_is_error() {
        let err = collect(2, vec![(5_i32, "x")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for out-of-bounds index"
        );
    }

    #[test]
    fn collect_batch_successes_duplicate_index_is_error() {
        let err = collect(3, vec![(1_i32, "a"), (1_i32, "b")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for duplicate index"
        );
    }

    fn make_success(index: usize) -> crate::BatchResourceSuccess<()> {
        crate::BatchResourceSuccess {
            request_index: index,
            item: (),
        }
    }

    fn make_domain_problem(index: usize) -> crate::BatchResourceProblem<()> {
        crate::BatchResourceProblem {
            request_index: index,
            error: (),
        }
    }

    #[test]
    fn validate_batch_response_indexes_happy_path() {
        let successes = vec![make_success(0), make_success(2)];
        let problems = vec![make_domain_problem(1)];
        assert!(validate_batch_response_indexes(&successes, &problems, 3, "test").is_ok());
    }

    #[test]
    fn validate_batch_response_indexes_overlap_is_error() {
        let successes = vec![make_success(0)];
        let problems = vec![make_domain_problem(0)];
        let err = validate_batch_response_indexes(&successes, &problems, 2, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error when success and problem share an index"
        );
    }

    #[test]
    fn validate_batch_response_indexes_duplicate_problem_index_is_error() {
        let successes: Vec<crate::BatchResourceSuccess<()>> = vec![];
        let problems = vec![make_domain_problem(0), make_domain_problem(0)];
        let err = validate_batch_response_indexes(&successes, &problems, 2, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for duplicate problem index"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
