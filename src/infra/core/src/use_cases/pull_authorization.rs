// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_accounts::Principal;
use kamu_core::*;
use kamu_datasets::DatasetOperationGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authorizes a pull plan: each job's operation on its target, with the inputs
/// a transform reads
pub(crate) struct PullAuthorization {
    dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
    principal: Arc<Principal>,
}

#[common_macros::method_names_consts]
impl PullAuthorization {
    pub(crate) fn new(
        dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
        principal: Arc<Principal>,
    ) -> Self {
        Self {
            dataset_operation_guard,
            principal,
        }
    }

    /// Removes the denied jobs from the plan and appends their responses to
    /// its errors, in job order. A plan that already has errors will not run
    /// and is returned unchecked. A sync into a dataset that does not exist
    /// yet is not checked.
    #[tracing::instrument(level = "debug", name = PullAuthorization_authorize, skip_all, fields(num_iterations = plan.iterations.len()))]
    pub(crate) async fn authorize(&self, plan: PullPlan) -> Result<PullPlan, InternalError> {
        if !plan.errors.is_empty() {
            return Ok(plan);
        }

        let PullPlan {
            iterations,
            mut errors,
        } = plan;

        let jobs_with_requests: Vec<_> = iterations
            .into_iter()
            .flat_map(|iteration| {
                let depth = iteration.depth;
                iteration.jobs.into_iter().map(move |job| {
                    let maybe_request = job.as_operation_request();
                    (depth, job, maybe_request)
                })
            })
            .collect();

        let requests: Vec<_> = jobs_with_requests
            .iter()
            .filter_map(|(_, _, maybe_request)| maybe_request.clone())
            .collect();
        let mut verdicts = self
            .dataset_operation_guard
            .check_operations(&self.principal, &requests)
            .await?
            .into_iter();

        let mut allowed_iterations: Vec<PullPlanIteration> = Vec::new();
        for (depth, job, maybe_request) in jobs_with_requests {
            if let Some(request) = maybe_request
                && let Err(error) = verdicts.next().expect("one verdict per request")
            {
                errors.push(PullResponse {
                    maybe_local_ref: Some(request.target.as_local_ref()),
                    maybe_remote_ref: None,
                    maybe_original_request: job.into_original_pull_request(),
                    result: Err(error.into()),
                });
                continue;
            }

            match allowed_iterations.last_mut() {
                Some(iteration) if iteration.depth == depth => iteration.jobs.push(job),
                Some(_) | None => allowed_iterations.push(PullPlanIteration {
                    depth,
                    jobs: vec![job],
                }),
            }
        }

        Ok(PullPlan {
            iterations: allowed_iterations,
            errors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
