// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common_macros::transactional_method1;
use internal_error::InternalError;
use kamu_core::*;
use kamu_datasets::{
    ClassifyByAllowanceDatasetActionUnauthorizedError,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
};

use super::PullDatasetUseCaseImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Authorization of a pull iteration: writing every target, reading every
/// input of a transform
impl PullDatasetUseCaseImpl {
    #[transactional_method1(dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>)]
    pub(super) async fn make_authorization_checks(
        &self,
        iteration: PullPlanIteration,
    ) -> Result<(PullPlanIteration, Vec<PullResponse>, Vec<PullResponse>), InternalError> {
        let (iteration, write_errors) = self
            .make_authorization_write_checks(iteration, dataset_action_authorizer.clone())
            .await?;
        let (iteration, read_errors) = self
            .make_authorization_read_checks(iteration, dataset_action_authorizer)
            .await?;
        Ok((iteration, write_errors, read_errors))
    }

    #[tracing::instrument(level = "debug", name = "PullDatasetUseCase::write_authorizations", skip_all, fields(?iteration))]
    async fn make_authorization_write_checks(
        &self,
        iteration: PullPlanIteration,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Result<(PullPlanIteration, Vec<PullResponse>), InternalError> {
        let mut written_datasets = Vec::with_capacity(iteration.jobs.len());
        let mut written_jobs_by_handle = HashMap::with_capacity(iteration.jobs.len());
        let mut other_jobs = Vec::new();

        for job in iteration.jobs {
            if let Some(written_handle) = job.as_common_item().try_get_written_handle() {
                written_datasets.push(written_handle.clone());
                written_jobs_by_handle.insert(written_handle.clone(), job);
            } else {
                other_jobs.push(job);
            }
        }

        if written_datasets.is_empty() {
            return Ok((
                PullPlanIteration {
                    depth: iteration.depth,
                    jobs: other_jobs,
                },
                Vec::new(),
            ));
        }

        let ClassifyByAllowanceResponse {
            authorized_handles,
            unauthorized_handles_with_errors,
        } = dataset_action_authorizer
            .classify_dataset_handles_by_allowance(written_datasets, DatasetAction::Write)
            .await?;

        let mut okay_jobs = Vec::with_capacity(authorized_handles.len() + other_jobs.len());
        for authorized_hdl in authorized_handles {
            let job = written_jobs_by_handle
                .remove(&authorized_hdl)
                .expect("item must be present");
            okay_jobs.push(job);
        }
        okay_jobs.extend(other_jobs);

        let unauthorized_responses: Vec<_> = unauthorized_handles_with_errors
            .into_iter()
            .map(|(hdl, auth_error)| {
                let job = written_jobs_by_handle
                    .remove(&hdl)
                    .expect("item must be present");
                PullResponse {
                    maybe_local_ref: Some(hdl.as_local_ref()),
                    maybe_remote_ref: None,
                    maybe_original_request: job.into_original_pull_request(),
                    result: Err(auth_error.into()),
                }
            })
            .collect();

        Ok((
            PullPlanIteration {
                depth: iteration.depth,
                jobs: okay_jobs,
            },
            unauthorized_responses,
        ))
    }

    #[tracing::instrument(level = "debug", name = "PullDatasetUseCase::read_authorizations", skip_all, fields(?iteration))]
    async fn make_authorization_read_checks(
        &self,
        iteration: PullPlanIteration,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Result<(PullPlanIteration, Vec<PullResponse>), InternalError> {
        let mut read_datasets = Vec::new();
        let mut reading_jobs = Vec::with_capacity(iteration.jobs.len());
        let mut other_jobs = Vec::with_capacity(iteration.jobs.len());

        for job in iteration.jobs {
            let read_handles = job.as_common_item().get_read_handles();
            if read_handles.is_empty() {
                other_jobs.push(job);
            } else {
                read_datasets.extend(read_handles.into_iter().cloned());
                reading_jobs.push(job);
            }
        }

        if read_datasets.is_empty() {
            return Ok((
                PullPlanIteration {
                    depth: iteration.depth,
                    jobs: other_jobs,
                },
                Vec::new(),
            ));
        }

        let ClassifyByAllowanceResponse {
            authorized_handles: _,
            unauthorized_handles_with_errors,
        } = dataset_action_authorizer
            .classify_dataset_handles_by_allowance(read_datasets, DatasetAction::Read)
            .await?;

        if unauthorized_handles_with_errors.is_empty() {
            let mut all_jobs = Vec::with_capacity(reading_jobs.len() + other_jobs.len());
            all_jobs.extend(reading_jobs);
            all_jobs.extend(other_jobs);
            return Ok((
                PullPlanIteration {
                    jobs: all_jobs,
                    depth: iteration.depth,
                },
                vec![],
            ));
        }

        let mut unauthorized_handles_to_errors: HashMap<
            odf::DatasetHandle,
            ClassifyByAllowanceDatasetActionUnauthorizedError,
        > = unauthorized_handles_with_errors.into_iter().collect();

        let mut unauthorized_responses = Vec::new();

        let mut okay_jobs = Vec::with_capacity(reading_jobs.len() + other_jobs.len());
        okay_jobs.extend(other_jobs);

        for reading_job in reading_jobs {
            let read_handles = reading_job.as_common_item().get_read_handles();
            let mut maybe_error = None;
            for read_hdl in read_handles {
                if let Some(auth_error) = unauthorized_handles_to_errors.remove(read_hdl) {
                    maybe_error = Some(PullError::from(auth_error));
                    break;
                }
            }

            if let Some(error) = maybe_error {
                unauthorized_responses.push(PullResponse {
                    maybe_local_ref: reading_job
                        .as_common_item()
                        .try_get_written_handle()
                        .map(odf::DatasetHandle::as_local_ref),
                    maybe_remote_ref: None,
                    maybe_original_request: reading_job.into_original_pull_request(),
                    result: Err(error),
                });
            } else {
                okay_jobs.push(reading_job);
            }
        }

        Ok((
            PullPlanIteration {
                depth: iteration.depth,
                jobs: okay_jobs,
            },
            unauthorized_responses,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
