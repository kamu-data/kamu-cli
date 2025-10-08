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

use database_common_macros::{
    transactional_method1,
    transactional_static_method1,
    transactional_static_method2,
};
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
    polling_ingest_service: Arc<dyn PollingIngestService>,
    sync_svc: Arc<dyn SyncService>,
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
                self.make_authorization_checks(iteration).await?;

            if !write_errors.is_empty() || !read_errors.is_empty() {
                results.extend(write_errors);
                results.extend(read_errors);
                break;
            }

            // Run iteration jobs concurrently
            let mut tasks = tokio::task::JoinSet::new();
            for job in iteration.jobs {
                match job {
                    PullPlanIterationJob::Ingest(pii) => {
                        let maybe_listener = maybe_ingest_multi_listener
                            .as_ref()
                            .and_then(|l| l.begin_ingest(pii.target.get_handle()));
                        let ingest_options = options.ingest_options.clone();

                        tasks.spawn(Self::ingest(
                            pii,
                            ingest_options,
                            self.catalog.clone(),
                            self.polling_ingest_service.clone(),
                            maybe_listener,
                        ))
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
                            self.catalog.clone(),
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

                        tasks.spawn(Self::sync(
                            psi,
                            sync_options,
                            self.sync_svc.clone(),
                            self.catalog.clone(),
                            maybe_listener,
                        ))
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

    #[transactional_method1(dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>)]
    async fn make_authorization_checks(
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

    async fn ingest_loop(
        pii: PullIngestItem,
        ingest_options: PollingIngestOptions,
        catalog: dill::Catalog,
        polling_ingest_svc: Arc<dyn PollingIngestService>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError> {
        let mut combined_result = None;
        let mut latest_metadata_state = pii.metadata_state;

        loop {
            match polling_ingest_svc
                .ingest(
                    pii.target.clone(),
                    latest_metadata_state.clone(),
                    ingest_options.clone(),
                    maybe_listener.clone(),
                )
                .await
            {
                Ok(ingest_res) => {
                    if let PollingIngestResult::Updated {
                        old_head,
                        new_head,
                        metadata_state,
                        ..
                    } = &ingest_res
                    {
                        Self::update_ref_transactionally(
                            catalog.clone(),
                            pii.target.get_handle(),
                            new_head,
                            old_head,
                        )
                        .await?;
                        latest_metadata_state = Box::new(metadata_state.clone());
                    }

                    combined_result = Some(Self::merge_results(combined_result, ingest_res));

                    let has_more = match combined_result {
                        Some(PollingIngestResult::UpToDate { .. }) => false,
                        Some(PollingIngestResult::Updated { has_more, .. }) => has_more,
                        None => unreachable!(),
                    };

                    if !has_more || !ingest_options.exhaust_sources {
                        break;
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(combined_result.unwrap())
    }

    async fn ingest(
        pii: PullIngestItem,
        ingest_options: PollingIngestOptions,
        catalog: dill::Catalog,
        polling_ingest_svc: Arc<dyn PollingIngestService>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PullResponse, InternalError> {
        let local_target = pii.target.get_handle().as_local_ref();
        let original_request = pii.maybe_original_request.clone();

        let ingest_response = Self::ingest_loop(
            pii,
            ingest_options,
            catalog,
            polling_ingest_svc,
            maybe_listener,
        )
        .await;

        Ok(PullResponse {
            maybe_original_request: original_request,
            maybe_local_ref: Some(local_target),
            maybe_remote_ref: None,
            result: match ingest_response {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        })
    }

    #[transactional_static_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn update_ref_transactionally(
        catalog: dill::Catalog,
        target_handle: &odf::DatasetHandle,
        new_head: &odf::Multihash,
        old_head: &odf::Multihash,
    ) -> Result<(), InternalError> {
        let transactional_target = dataset_registry.get_dataset_by_handle(target_handle).await;

        // ToDo: Handle concurrent `HEAD` updates
        transactional_target
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
            .int_err()
    }

    #[transactional_static_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn elaborate_transform(
        mut pti: PullTransformItem,
        transform_options: TransformOptions,
        transform_elaboration_svc: Arc<dyn TransformElaborationService>,
        catalog: dill::Catalog,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        pti.refresh_from_dataset_registry(&*dataset_registry)
            .await
            .int_err()?;

        let mut result = transform_elaboration_svc
            .elaborate_transform(
                pti.target.clone(),
                pti.plan,
                transform_options,
                maybe_listener.clone(),
            )
            .await;

        if let Ok(TransformElaboration::Elaborated(elaborate_res)) = &mut result {
            elaborate_res.datasets_map.detach_from_transaction();
        }

        result
    }

    async fn transform(
        pti: PullTransformItem,
        transform_options: TransformOptions,
        transform_elaboration_svc: Arc<dyn TransformElaborationService>,
        transform_executor: Arc<dyn TransformExecutor>,
        catalog: dill::Catalog,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Remember original request
        let maybe_original_request = pti.maybe_original_request.clone();

        // Remember original target
        let pti_target = pti.target.clone();

        let transform_result = match Self::elaborate_transform(
            pti,
            transform_options,
            transform_elaboration_svc,
            catalog.clone(),
            maybe_listener.clone(),
        )
        .await
        {
            // Elaborate success
            Ok(TransformElaboration::Elaborated(plan)) => {
                // Execute phase
                let (target, result) = transform_executor
                    .execute_transform(pti_target.clone(), plan, maybe_listener)
                    .await;
                if let Ok(TransformResult::Updated {
                    old_head, new_head, ..
                }) = &result
                {
                    Self::update_ref_transactionally(
                        catalog.clone(),
                        target.get_handle(),
                        new_head,
                        old_head,
                    )
                    .await?;
                }
                result.map_err(|e| PullError::TransformError(TransformError::Execute(e)))
            }
            // Already up-to-date
            Ok(TransformElaboration::UpToDate) => Ok(TransformResult::UpToDate),
            // Elaborate error
            Err(e) => Err(PullError::TransformError(TransformError::Elaborate(e))),
        };

        // Prepare response
        Ok(PullResponse {
            maybe_original_request,
            maybe_local_ref: Some(pti_target.get_handle().as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.map(Into::into),
        })
    }

    async fn sync(
        psi: PullSyncItem,
        options: PullOptions,
        sync_svc: Arc<dyn SyncService>,
        catalog: dill::Catalog,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResponse, InternalError> {
        let mut sync_result = sync_svc
            .sync(*psi.sync_request, options.sync_options, listener)
            .await;

        // Associate newly-synced datasets with remotes
        if options.add_aliases
            && let Ok(SyncResult::Updated { old_head: None, .. }) = &sync_result
            && let Err(e) = Self::store_dataset_alias_transactional(
                catalog,
                &psi.local_target.as_local_ref(),
                &psi.remote_ref,
            )
            .await
        {
            sync_result = Err(SyncError::Internal(e));
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

    // TODO: Introduce intermediate structs to avoid full unpacking
    fn merge_results(
        combined_result: Option<PollingIngestResult>,
        new_result: PollingIngestResult,
    ) -> PollingIngestResult {
        match (combined_result, new_result) {
            (None | Some(PollingIngestResult::UpToDate { .. }), n) => n,
            (
                Some(PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    metadata_state,
                    ..
                }),
                PollingIngestResult::UpToDate { uncacheable, .. },
            ) => PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more: false,
                uncacheable,
                metadata_state,
            },
            (
                Some(PollingIngestResult::Updated {
                    old_head: prev_old_head,
                    ..
                }),
                PollingIngestResult::Updated {
                    new_head,
                    has_more,
                    uncacheable,
                    metadata_state,
                    ..
                },
            ) => PollingIngestResult::Updated {
                old_head: prev_old_head,
                new_head,
                has_more,
                uncacheable,
                metadata_state,
            },
        }
    }

    #[transactional_method1(pull_request_planner: Arc<dyn PullRequestPlanner>)]
    async fn build_plan_for_requests(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
    ) -> Result<
        (
            Vec<kamu_core::PullPlanIteration>,
            Vec<kamu_core::PullResponse>,
        ),
        InternalError,
    > {
        let res = pull_request_planner
            .build_pull_multi_plan(requests, options, *self.tenancy_config)
            .await;

        // Detach plan entities from current transaction
        // to be able to commit it and manage them in iterations separately
        for iteration in &res.0 {
            for job in &iteration.jobs {
                job.detach_from_transaction();
            }
        }
        Ok(res)
    }

    #[transactional_method1(pull_request_planner: Arc<dyn PullRequestPlanner>)]
    async fn build_plan_for_all_owned_datasets(
        &self,
        options: &PullOptions,
    ) -> Result<
        (
            Vec<kamu_core::PullPlanIteration>,
            Vec<kamu_core::PullResponse>,
        ),
        InternalError,
    > {
        let res = pull_request_planner
            .build_pull_plan_all_owner_datasets(options, *self.tenancy_config)
            .await?;

        // Detach plan entities from current transaction
        // to be able to commit it and manage them in iterations separately
        for iteration in &res.0 {
            for job in &iteration.jobs {
                job.detach_from_transaction();
            }
        }

        Ok(res)
    }

    #[transactional_static_method2(dataset_registry: Arc<dyn DatasetRegistry>, remote_alias_registry: Arc<dyn RemoteAliasesRegistry>)]
    async fn store_dataset_alias_transactional(
        catalog: dill::Catalog,
        local_ref: &odf::DatasetRef,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<bool, InternalError> {
        // Note: this would have failed before sync if dataset didn't exist,
        // however, by this moment the dataset must have been created
        let hdl = dataset_registry
            .resolve_dataset_handle_by_ref(local_ref)
            .await
            .int_err()?;

        match remote_alias_registry.get_remote_aliases(&hdl).await {
            Ok(mut aliases) => aliases.add(remote_ref, RemoteAliasKind::Pull).await,
            Err(e) => match e {
                GetAliasesError::Internal(e) => Err(e),
            },
        }
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

        let (plan, errors) = self
            .build_plan_for_requests(requests.as_slice(), &options)
            .await?;

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

        let (plan, errors) = self.build_plan_for_all_owned_datasets(&options).await?;

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
