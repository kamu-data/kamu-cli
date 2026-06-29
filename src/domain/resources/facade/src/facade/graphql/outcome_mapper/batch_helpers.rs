// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use super::problem_mappers::map_batch_lookup_problem;
use crate::facade::graphql::cynic_api;
use crate::{
    BatchResourceError,
    BatchResourceProblem,
    BatchResourceSuccess,
    ResourceLookupProblem,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn collect_batch_successes<S, T, F>(
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

pub(crate) fn collect_batch_problems(
    problems: Vec<cynic_api::fragments::BatchResourceProblem>,
    request_len: usize,
    context: &str,
) -> Result<Vec<BatchResourceProblem<ResourceLookupProblem>>, BatchResourceError> {
    problems
        .into_iter()
        .map(|problem| {
            let request_index = usize::try_from(problem.request_index).map_err(|_| {
                BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} problem index {} cannot be converted to usize",
                    problem.request_index
                )))
            })?;
            if request_index >= request_len {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} problem index {request_index} is out of bounds",
                ))));
            }
            let error = map_batch_lookup_problem(problem.problem)?;
            Ok(BatchResourceProblem {
                request_index,
                error,
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_batch_response_indexes<T, E>(
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

#[cfg(test)]
mod tests {
    use kamu_resources::ResourceID;

    use super::*;
    use crate::facade::graphql::cynic_api::fragments::{
        BatchResourceProblem,
        ResourceIDNotFoundProblem,
        ResourceLookupProblem as CynicResourceLookupProblem,
    };

    fn id_not_found_problem(index: i32) -> BatchResourceProblem {
        let id: ResourceID =
            serde_json::from_str("\"00000000-0000-0000-0000-000000000000\"").unwrap();
        BatchResourceProblem {
            request_index: index,
            problem: CynicResourceLookupProblem::ResourceIDNotFoundProblem(
                ResourceIDNotFoundProblem { id },
            ),
        }
    }

    #[test]
    fn collect_batch_problems_negative_index_is_error() {
        let err = collect_batch_problems(vec![id_not_found_problem(-1)], 1, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for negative problem index"
        );
    }

    #[test]
    fn collect_batch_problems_out_of_bounds_index_is_error() {
        let err = collect_batch_problems(vec![id_not_found_problem(5)], 1, "test").unwrap_err();
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
