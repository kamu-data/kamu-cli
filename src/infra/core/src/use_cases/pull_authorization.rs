// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
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

    /// Checks every job of the plan. A denied job the caller named becomes an
    /// error response of the plan. A denied dependency, which a recursive
    /// pull added, leaves the plan, and its dependents read its current
    /// state. A plan that already has errors will not run and is left
    /// unchecked. A sync into a dataset that does not exist yet is not
    /// checked.
    #[tracing::instrument(level = "debug", name = PullAuthorization_authorize, skip_all, fields(num_iterations = plan.iterations.len()))]
    pub(crate) async fn authorize(&self, plan: &mut PullPlan) -> Result<(), InternalError> {
        if !plan.errors.is_empty() {
            return Ok(());
        }

        let checked_jobs: Vec<_> = plan
            .iterations
            .iter()
            .flat_map(|iteration| &iteration.jobs)
            .filter_map(|job| job.as_operation_request().map(|request| (job, request)))
            .collect();
        let requests: Vec<_> = checked_jobs
            .iter()
            .map(|(_, request)| request.clone())
            .collect();

        let verdicts = self
            .dataset_operation_guard
            .check_operations(&self.principal, &requests)
            .await?;

        let mut skipped_dependencies = HashSet::new();
        for ((job, request), verdict) in checked_jobs.into_iter().zip(verdicts) {
            let Err(error) = verdict else {
                continue;
            };
            if let Some(original_request) = job.original_pull_request() {
                plan.errors.push(PullResponse {
                    maybe_local_ref: Some(request.target.as_local_ref()),
                    maybe_remote_ref: None,
                    maybe_original_request: Some(original_request.clone()),
                    result: Err(error.into()),
                });
            } else {
                tracing::info!(dataset = %request.target, %error, "Skipping a dependency the caller may not pull");
                skipped_dependencies.insert(request.target.id);
            }
        }

        if !skipped_dependencies.is_empty() {
            for iteration in &mut plan.iterations {
                iteration.jobs.retain(|job| {
                    job.existing_target()
                        .is_none_or(|target| !skipped_dependencies.contains(&target.id))
                });
            }
            plan.iterations
                .retain(|iteration| !iteration.jobs.is_empty());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
