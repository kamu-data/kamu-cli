// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use internal_error::InternalError;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::DatasetOperationAuthorizer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authorizes a pull iteration: each job's operation on its target, with the
/// inputs a transform reads
pub(crate) struct PullAuthorization {
    catalog: dill::Catalog,
}

/// The jobs of an iteration that may run, and the responses of those denied
pub(crate) struct AuthorizedIteration {
    pub iteration: PullPlanIteration,
    pub denied: Vec<PullResponse>,
}

impl PullAuthorization {
    pub(crate) fn new(catalog: dill::Catalog) -> Self {
        Self { catalog }
    }

    /// Denials come in job order. A sync into a dataset that does not exist
    /// yet is not checked.
    #[transactional_method2(dataset_operation_authorizer: Arc<dyn DatasetOperationAuthorizer>, current_account_subject: Arc<CurrentAccountSubject>)]
    #[tracing::instrument(level = "debug", name = "PullDatasetUseCase::authorize", skip_all, fields(?iteration))]
    pub(crate) async fn authorize(
        &self,
        iteration: PullPlanIteration,
    ) -> Result<AuthorizedIteration, InternalError> {
        let jobs_with_requests: Vec<_> = iteration
            .jobs
            .into_iter()
            .map(|job| {
                let maybe_request = job.as_operation_request();
                (job, maybe_request)
            })
            .collect();

        let requests: Vec<_> = jobs_with_requests
            .iter()
            .filter_map(|(_, maybe_request)| maybe_request.clone())
            .collect();
        let mut verdicts = dataset_operation_authorizer
            .authorize_operations(&current_account_subject, &requests)
            .await?
            .into_iter();

        let mut jobs = Vec::with_capacity(jobs_with_requests.len());
        let mut denied = Vec::new();
        for (job, maybe_request) in jobs_with_requests {
            let Some(request) = maybe_request else {
                jobs.push(job);
                continue;
            };

            match verdicts.next().expect("one verdict per request") {
                Ok(()) => jobs.push(job),
                Err(error) => denied.push(PullResponse {
                    maybe_local_ref: Some(request.target.as_local_ref()),
                    maybe_remote_ref: None,
                    maybe_original_request: job.into_original_pull_request(),
                    result: Err(error.into()),
                }),
            }
        }

        Ok(AuthorizedIteration {
            iteration: PullPlanIteration {
                depth: iteration.depth,
                jobs,
            },
            denied,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
