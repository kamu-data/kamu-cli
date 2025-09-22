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

use database_common::DatabaseTransactionRunner;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::auth::{
    ClassifyByAllowanceDatasetActionUnauthorizedError,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
};
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PullDatasetUseCase)]
pub struct PullDatasetUseCaseImpl {
    transform_elaboration_svc: Arc<dyn TransformElaborationService>,
    transform_executor: Arc<dyn TransformExecutor>,
    tenancy_config: Arc<TenancyConfig>,
    catalog: dill::Catalog,
}

impl PullDatasetUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn pull_by_plan(
        &self,
        plan: Vec<PullPlanIteration>,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut results = Vec::new();

        // Prepare multi-listeners
        let maybe_ingest_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_ingest_listener());

        let maybe_transform_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_transform_listener());

        let maybe_sync_multi_listener = listener
            .as_ref()
            .and_then(|l| l.clone().get_sync_listener());

        // Execute each iteration
        for iteration in plan {
            tracing::info!(depth = %iteration.depth, jobs = ?iteration.jobs, "Running pull iteration");

            // Authorization checks for this iteration
            let (iteration, write_errors, read_errors) =
                DatabaseTransactionRunner::new(self.catalog.clone())
                    .transactional_with(
                        |dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>| async move {
                            let (iteration, write_errors) = Self::make_authorization_write_checks(
                                iteration,
                                dataset_action_authorizer.clone(),
                            )
                            .await?;
                            let (iteration, read_errors) = Self::make_authorization_read_checks(
                                iteration,
                                dataset_action_authorizer,
                            )
                            .await?;
                            Ok((iteration, write_errors, read_errors))
                        },
                    )
                    .await?;

            if !write_errors.is_empty() || !read_errors.is_empty() {
                results.extend(write_errors);
                results.extend(read_errors);
                break;
            }

            println!("qweqweqwe");

            // Run iteration jobs concurrently
            let mut tasks = tokio::task::JoinSet::new();
            for job in iteration.jobs {
                let catalog = self.catalog.clone();

                match job {
                    PullPlanIterationJob::Ingest(pii) => {
                        let maybe_listener = maybe_ingest_multi_listener
                            .as_ref()
                            .and_then(|l| l.begin_ingest(pii.target.get_handle()));
                        let ingest_options = options.ingest_options.clone();

                        tasks.spawn(async move {
                            DatabaseTransactionRunner::new(catalog)
                                .transactional_with(
                                    |polling_ingest_svc: Arc<dyn PollingIngestService>| async move {
                                        Self::ingest(
                                            pii,
                                            ingest_options,
                                            polling_ingest_svc.clone(),
                                            maybe_listener,
                                        )
                                        .await
                                    },
                                )
                                .await
                        })
                    }
                    PullPlanIterationJob::Transform(pti) => {
                        let maybe_listener = maybe_transform_multi_listener
                            .as_ref()
                            .and_then(|l| l.begin_transform(pti.target.get_handle()));

                        tasks.spawn(Self::transform(
                            pti,
                            options.transform_options,
                            self.transform_elaboration_svc.clone(),
                            self.transform_executor.clone(),
                            maybe_listener,
                        ))
                    }
                    PullPlanIterationJob::Sync(psi) => {
                        let maybe_listener = maybe_sync_multi_listener.as_ref().and_then(|l| {
                            l.begin_sync(
                                &psi.sync_request.src.as_user_friendly_any_ref(),
                                &psi.sync_request.dst.as_user_friendly_any_ref(),
                            )
                        });
                        let sync_options = options.clone();

                        tasks.spawn(async move {
                            DatabaseTransactionRunner::new(catalog)
                                .transactional_with3(
                                    |sync_svc: Arc<dyn SyncService>, dataset_registry: Arc<dyn DatasetRegistry>, remote_alias_registry: Arc<dyn RemoteAliasesRegistry>| async move {
                                        Self::sync(
                                            psi,
                                            sync_options,
                                            sync_svc.clone(),
                                            dataset_registry.clone(),
                                            remote_alias_registry.clone(),
                                            maybe_listener,
                                        )
                                        .await
                                    },
                                )
                                .await
                        })

                        // tasks.spawn(Self::sync(
                        //     psi,
                        //     options.clone(),
                        //     self.sync_svc.get().unwrap().clone(),
                        //     self.dataset_registry.get().unwrap().clone(),
                        //     self.remote_alias_registry.get().unwrap().
                        // clone(),     maybe_listener,
                        // ))
                    }
                };
            }
            let iteration_results = tasks.join_all().await;
            tracing::info!(iteration_result=?iteration_results, "Pull iteration finished");

            // Deal with results
            let mut has_errors = false;
            for result in iteration_results {
                let result = result?;
                if result.result.is_err() {
                    has_errors = true;
                }
                results.push(result);
            }
            if has_errors {
                break;
            }
        }

        Ok(results)
    }

    #[tracing::instrument(level = "debug", name = "PullDatasetUseCase::write_authorizations", skip_all, fields(?iteration))]
    async fn make_authorization_write_checks(
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
                    result: Err({
                        use ClassifyByAllowanceDatasetActionUnauthorizedError as E;

                        match auth_error {
                            E::NotFound(e) => PullError::NotFound(e),
                            E::Access(e) => PullError::Access(e),
                            E::Internal(e) => PullError::Internal(e),
                        }
                    }),
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
                    maybe_error = Some({
                        use ClassifyByAllowanceDatasetActionUnauthorizedError as E;

                        match auth_error {
                            E::NotFound(e) => PullError::NotFound(e),
                            E::Access(e) => PullError::Access(e),
                            E::Internal(e) => PullError::Internal(e),
                        }
                    });
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

    async fn ingest(
        pii: PullIngestItem,
        ingest_options: PollingIngestOptions,
        polling_ingest_svc: Arc<dyn PollingIngestService>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PullResponse, InternalError> {
        let ingest_response = polling_ingest_svc
            .ingest(
                pii.target.clone(),
                pii.metadata_state,
                ingest_options,
                maybe_listener,
            )
            .await;

        if let Ok(PollingIngestResult::Updated {
            old_head, new_head, ..
        }) = &ingest_response
        {
            pii.target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .int_err()?;
        }

        Ok(PullResponse {
            maybe_original_request: pii.maybe_original_request,
            maybe_local_ref: Some(pii.target.get_handle().as_local_ref()),
            maybe_remote_ref: None,
            result: match ingest_response {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        })
    }

    async fn transform(
        pti: PullTransformItem,
        transform_options: TransformOptions,
        transform_elaboration_svc: Arc<dyn TransformElaborationService>,
        transform_executor: Arc<dyn TransformExecutor>,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Remember original request
        let maybe_original_request = pti.maybe_original_request.clone();

        // Remember original target
        let pti_target = pti.target.clone();

        // Main transform run
        async fn run_transform(
            pti: PullTransformItem,
            transform_elaboration_svc: Arc<dyn TransformElaborationService>,
            transform_executor: Arc<dyn TransformExecutor>,
            transform_options: TransformOptions,
            maybe_listener: Option<Arc<dyn TransformListener>>,
        ) -> (ResolvedDataset, Result<TransformResult, PullError>) {
            // Elaborate phase
            match transform_elaboration_svc
                .elaborate_transform(
                    pti.target.clone(),
                    pti.plan,
                    transform_options,
                    maybe_listener.clone(),
                )
                .await
            {
                // Elaborate success
                Ok(TransformElaboration::Elaborated(plan)) => {
                    // Execute phase
                    let (target, result) = transform_executor
                        .execute_transform(pti.target, plan, maybe_listener)
                        .await;
                    (
                        target,
                        result.map_err(|e| PullError::TransformError(TransformError::Execute(e))),
                    )
                }
                // Already up-to-date
                Ok(TransformElaboration::UpToDate) => (pti.target, Ok(TransformResult::UpToDate)),
                // Elaborate error
                Err(e) => (
                    pti.target,
                    Err(PullError::TransformError(TransformError::Elaborate(e))),
                ),
            }
        }

        let transform_result = run_transform(
            pti,
            transform_elaboration_svc,
            transform_executor,
            transform_options,
            maybe_listener,
        )
        .await;

        if let Ok(TransformResult::Updated {
            old_head, new_head, ..
        }) = &transform_result.1
        {
            pti_target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    new_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: Some(Some(old_head)),
                    },
                )
                .await
                .int_err()?;
        }

        // Prepare response
        Ok(PullResponse {
            maybe_original_request,
            maybe_local_ref: Some(transform_result.0.get_handle().as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.1.map(Into::into),
        })
    }

    async fn sync(
        psi: PullSyncItem,
        options: PullOptions,
        sync_svc: Arc<dyn SyncService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        remote_alias_registry: Arc<dyn RemoteAliasesRegistry>,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Run sync action
        let mut sync_result = sync_svc
            .sync(*psi.sync_request, options.sync_options, listener)
            .await;

        // Associate newly-synced datasets with remotes
        if options.add_aliases
            && let Ok(SyncResult::Updated { old_head: None, .. }) = &sync_result
        {
            // Note: this would have failed before sync if dataset didn't exist,
            // however, by this moment the dataset must have been created
            let hdl = dataset_registry
                .resolve_dataset_handle_by_ref(&psi.local_target.as_local_ref())
                .await
                .int_err()?;

            let alias_add_result = match remote_alias_registry.get_remote_aliases(&hdl).await {
                Ok(mut aliases) => aliases.add(&psi.remote_ref, RemoteAliasKind::Pull).await,
                Err(e) => match e {
                    GetAliasesError::Internal(e) => Err(e),
                },
            };

            if let Err(e) = alias_add_result {
                sync_result = Err(SyncError::Internal(e));
            }
        }

        // Prepare response
        Ok(PullResponse {
            maybe_original_request: psi.maybe_original_request,
            maybe_local_ref: Some(psi.local_target.as_local_ref()), // TODO: multi-tenancy
            maybe_remote_ref: Some(psi.remote_ref),
            result: match sync_result {
                Ok(response) => Ok(response.into()),
                Err(e) => Err(e.into()),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PullDatasetUseCase for PullDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute",
        skip_all,
        fields(?request, ?options)
    )]
    async fn execute(
        &self,
        request: PullRequest,
        options: PullOptions,
        listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResponse, InternalError> {
        let listener =
            listener.map(|l| Arc::new(ListenerMultiAdapter(l)) as Arc<dyn PullMultiListener>);

        let mut responses = self.execute_multi(vec![request], options, listener).await?;

        assert_eq!(responses.len(), 1);
        Ok(responses.pop().unwrap())
    }

    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute_multi",
        skip_all,
        fields(?requests, ?options)
    )]
    async fn execute_multi(
        &self,
        requests: Vec<PullRequest>,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        tracing::info!(?requests, ?options, "Performing pull");

        let cloned_options = options.clone();
        let (plan, errors) = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |pull_request_planner: Arc<dyn PullRequestPlanner>| async move {
                    Ok::<
                        (
                            Vec<kamu_core::PullPlanIteration>,
                            Vec<kamu_core::PullResponse>,
                        ),
                        InternalError,
                    >(
                        pull_request_planner
                            .build_pull_multi_plan(&requests, &cloned_options, *self.tenancy_config)
                            .await,
                    )
                },
            )
            .await
            .unwrap();

        tracing::info!(
            num_steps = plan.len(),
            num_errors = errors.len(),
            "Prepared pull execution plan"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        self.pull_by_plan(plan, options, listener).await
    }

    #[tracing::instrument(
        level = "info",
        name = "PullDatasetUseCase::execute_all_owned",
        skip_all,
        fields(?options)
    )]
    async fn execute_all_owned(
        &self,
        options: PullOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        tracing::info!(?options, "Performing pull (all owned)");

        let cloned_options = options.clone();
        let (plan, errors) = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |pull_request_planner: Arc<dyn PullRequestPlanner>| async move {
                    pull_request_planner
                        .build_pull_plan_all_owner_datasets(&cloned_options, *self.tenancy_config)
                        .await
                },
            )
            .await?;

        tracing::info!(
            num_steps = plan.len(),
            num_errors = errors.len(),
            "Prepared pull execution plan (all owned)"
        );
        if !errors.is_empty() {
            return Ok(errors);
        }

        self.pull_by_plan(plan, options, listener).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ListenerMultiAdapter(Arc<dyn PullListener>);

impl PullMultiListener for ListenerMultiAdapter {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestMultiListener>> {
        Some(self)
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformMultiListener>> {
        Some(self)
    }

    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncMultiListener>> {
        Some(self)
    }
}

impl PollingIngestMultiListener for ListenerMultiAdapter {
    fn begin_ingest(
        &self,
        _dataset: &odf::DatasetHandle,
    ) -> Option<Arc<dyn PollingIngestListener>> {
        self.0.clone().get_ingest_listener()
    }
}

impl TransformMultiListener for ListenerMultiAdapter {
    fn begin_transform(&self, _dataset: &odf::DatasetHandle) -> Option<Arc<dyn TransformListener>> {
        self.0.clone().get_transform_listener()
    }
}

impl SyncMultiListener for ListenerMultiAdapter {
    fn begin_sync(
        &self,
        _src: &odf::DatasetRefAny,
        _dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        self.0.clone().get_sync_listener()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
