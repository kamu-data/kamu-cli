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
use kamu_core::*;
use kamu_datasets::{
    ClassifyByAllowanceDatasetActionUnauthorizedError,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetHeadCommitter,
    DatasetRegistry,
    ResolvedDataset,
};
use random_strings::get_random_name;

use super::pull_ingest_loop::PullIngestLoop;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PullDatasetUseCase)]
pub struct PullDatasetUseCaseImpl {
    transform_executor: Arc<dyn TransformExecutor>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    tenancy_config: Arc<TenancyConfig>,
    polling_ingest_service: Arc<dyn PollingIngestService>,
    sync_svc: Arc<dyn SyncService>,
    catalog: dill::Catalog,
}

impl PullDatasetUseCaseImpl {
    fn transform_services(&self) -> TransformServices {
        TransformServices {
            executor: self.transform_executor.clone(),
            compaction_planner: self.compaction_planner.clone(),
            compaction_executor: self.compaction_executor.clone(),
        }
    }

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
                        tasks.spawn(Self::ingest(
                            pii,
                            options.ingest_options.clone(),
                            options.exhaust_sources,
                            PullIngestLoop::new(
                                self.catalog.clone(),
                                self.polling_ingest_service.clone(),
                            ),
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
                            self.transform_services(),
                            self.catalog.clone(),
                            maybe_listener,
                        ))
                    }
                    PullPlanIterationJob::Sync(psi) => {
                        let maybe_listener = maybe_sync_multi_listener.as_ref().and_then(|l| {
                            l.begin_sync(
                                &psi.sync_request.src.as_user_friendly_any_ref(),
                                &psi.sync_request.dst.as_any_ref(),
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

    async fn ingest(
        pii: PullIngestItem,
        ingest_options: PollingIngestOptions,
        exhaust_sources: bool,
        ingest_loop: PullIngestLoop,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PullResponse, InternalError> {
        let target_handle = pii.target.get_handle();

        let result = ingest_loop
            .run(
                target_handle,
                ingest_options,
                exhaust_sources,
                maybe_listener,
            )
            .await
            .map(Into::into);

        Ok(PullResponse {
            maybe_original_request: pii.maybe_original_request,
            maybe_local_ref: Some(target_handle.as_local_ref()),
            maybe_remote_ref: None,
            result,
        })
    }

    #[transactional_static_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_head_transactionally(
        catalog: dill::Catalog,
        target_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let transactional_target = dataset_registry.get_dataset_by_handle(target_handle).await;

        dataset_head_committer
            .commit_head(&transactional_target, Some(old_head), new_head)
            .await
    }

    /// Elaborates in one transaction, then detaches the plan for execution
    #[transactional_static_method2(dataset_registry: Arc<dyn DatasetRegistry>, transform_planner: Arc<dyn TransformPlanner>)]
    async fn elaborate_transform(
        target_handle: &odf::DatasetHandle,
        catalog: dill::Catalog,
    ) -> Result<TransformElaboration, TransformElaborateError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;

        let elaboration = transform_planner.elaborate_transform(target).await?;
        if let TransformElaboration::Elaborated(plan) = &elaboration {
            plan.datasets_map.detach_from_transaction();
        }

        Ok(elaboration)
    }

    #[transactional_static_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn plan_reset_to_metadata(
        target_handle: &odf::DatasetHandle,
        compaction_planner: Arc<dyn CompactionPlanner>,
        catalog: dill::Catalog,
    ) -> Result<(ResolvedDataset, CompactionPlan), TransformError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;
        let plan = compaction_planner
            .plan_compaction(
                target.clone(),
                CompactionOptions {
                    keep_metadata_only: true,
                    ..Default::default()
                },
                None,
            )
            .await
            .int_err()?;

        target.detach_from_transaction();
        Ok((target, plan))
    }

    /// Resets the derivative to metadata, returning the heads before and
    /// after, or `None` when there was nothing to reset.
    async fn reset_to_metadata(
        target_handle: &odf::DatasetHandle,
        services: &TransformServices,
        catalog: dill::Catalog,
    ) -> Result<Option<(odf::Multihash, odf::Multihash)>, TransformError> {
        let (target, plan) = Self::plan_reset_to_metadata(
            target_handle,
            services.compaction_planner.clone(),
            catalog.clone(),
        )
        .await?;

        match services
            .compaction_executor
            .execute(target, plan, None)
            .await
            .int_err()?
        {
            CompactionResult::NothingToDo => Ok(None),
            CompactionResult::Success {
                old_head, new_head, ..
            } => {
                Self::commit_head_transactionally(catalog, target_handle, &old_head, &new_head)
                    .await
                    .map_err(|e| TransformExecuteError::CommitError(e.into()))?;
                Ok(Some((old_head, new_head)))
            }
        }
    }

    async fn transform(
        pti: PullTransformItem,
        transform_options: TransformOptions,
        services: TransformServices,
        catalog: dill::Catalog,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        // Remember original request
        let maybe_original_request = pti.maybe_original_request.clone();

        // Remember original target
        let target_handle = pti.target.get_handle().clone();

        let transform_result = Self::elaborate_and_execute_transform(
            pti,
            transform_options,
            services,
            catalog,
            maybe_listener,
        )
        .await;

        // Prepare response
        Ok(PullResponse {
            maybe_original_request,
            maybe_local_ref: Some(target_handle.as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.map(Into::into),
        })
    }

    /// The pull authorized reading the inputs of the transform it planned.
    /// Another writer may change the transform before the job elaborates it,
    /// so inputs the pull did not plan are authorized here, before any is read
    #[transactional_static_method1(dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>)]
    async fn authorize_unplanned_inputs(
        plan: &TransformExecutionPlan,
        planned_inputs: &[odf::DatasetHandle],
        catalog: dill::Catalog,
    ) -> Result<(), PullError> {
        let unplanned_inputs: Vec<_> = plan
            .request
            .inputs
            .iter()
            .map(|input| &input.dataset_handle)
            .filter(|handle| !planned_inputs.iter().any(|planned| planned.id == handle.id))
            .cloned()
            .collect();
        if unplanned_inputs.is_empty() {
            return Ok(());
        }

        let ClassifyByAllowanceResponse {
            authorized_handles: _,
            unauthorized_handles_with_errors,
        } = dataset_action_authorizer
            .classify_dataset_handles_by_allowance(unplanned_inputs, DatasetAction::Read)
            .await?;

        match unauthorized_handles_with_errors.into_iter().next() {
            None => Ok(()),
            Some((_, error)) => {
                use ClassifyByAllowanceDatasetActionUnauthorizedError as E;
                Err(match error {
                    E::NotFound(e) => PullError::NotFound(e),
                    E::Access(e) => PullError::Access(e),
                    E::Internal(e) => PullError::Internal(e),
                })
            }
        }
    }

    async fn elaborate_and_execute_transform(
        pti: PullTransformItem,
        transform_options: TransformOptions,
        services: TransformServices,
        catalog: dill::Catalog,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, PullError> {
        let listener = maybe_listener
            .clone()
            .unwrap_or_else(|| Arc::new(NullTransformListener));
        let target = pti.target.clone();
        let mut maybe_reset_heads = None;

        let elaboration = match Self::elaborate_transform(target.get_handle(), catalog.clone())
            .await
        {
            Err(TransformElaborateError::InvalidInputInterval(e))
                if transform_options.reset_derivatives_on_diverged_input =>
            {
                tracing::warn!(error = %e, "Input diverged, resetting the derivative to metadata");

                match Self::reset_to_metadata(target.get_handle(), &services, catalog.clone())
                    .await
                    .map_err(PullError::TransformError)?
                {
                    Some(reset_heads) => {
                        maybe_reset_heads = Some(reset_heads);
                        Self::elaborate_transform(target.get_handle(), catalog.clone()).await
                    }
                    None => Err(TransformElaborateError::InvalidInputInterval(e)),
                }
            }
            result => result,
        };

        let (transform_result, elaborated_up_to_date) = match elaboration {
            Ok(TransformElaboration::Elaborated(plan)) => {
                Self::authorize_unplanned_inputs(&plan, &pti.input_handles, catalog.clone())
                    .await?;
                (
                    Self::execute_transform_and_commit(
                        target,
                        plan,
                        &services,
                        catalog,
                        maybe_listener,
                    )
                    .await?,
                    false,
                )
            }
            Ok(TransformElaboration::UpToDate) => (TransformResult::UpToDate, true),
            Err(e) => {
                listener.elaborate_error(&e);
                return Err(TransformError::from(e).into());
            }
        };

        // A reset and the transform after it are reported as one update. The
        // offsets and the increment stay the transform's own: the reset
        // rewrote history, so no interval spans both
        let transform_result = match (maybe_reset_heads, transform_result) {
            (
                Some((old_head, _)),
                TransformResult::Updated {
                    new_head,
                    new_offset_interval,
                    increment,
                    ..
                },
            ) => TransformResult::Updated {
                old_head,
                new_head,
                new_offset_interval,
                increment,
            },
            (Some((old_head, new_head)), TransformResult::UpToDate) => TransformResult::Updated {
                old_head,
                new_head,
                new_offset_interval: None,
                increment: odf::dataset::MetadataChainIncrementInterval::default(),
            },
            (None, transform_result) => transform_result,
        };

        // The executor reports its own outcomes
        if elaborated_up_to_date {
            listener.success(&transform_result);
        }

        Ok(transform_result)
    }

    async fn execute_transform_and_commit(
        target: ResolvedDataset,
        plan: TransformExecutionPlan,
        services: &TransformServices,
        catalog: dill::Catalog,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformError> {
        let (target, result) = services
            .executor
            .execute_transform(target, plan, &get_random_name(None, 10), maybe_listener)
            .await;

        let result = result?;
        if let TransformResult::Updated {
            old_head, new_head, ..
        } = &result
        {
            Self::commit_head_transactionally(catalog, target.get_handle(), old_head, new_head)
                .await
                .map_err(|e| TransformExecuteError::CommitError(e.into()))?;
        }

        Ok(result)
    }

    async fn sync(
        psi: PullSyncItem,
        options: PullOptions,
        sync_svc: Arc<dyn SyncService>,
        catalog: dill::Catalog,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResponse, InternalError> {
        let mut sync_result = Self::transfer_and_commit(
            *psi.sync_request,
            options.sync_options,
            sync_svc,
            catalog.clone(),
            listener.clone(),
        )
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

        if let Some(listener) = &listener {
            match &sync_result {
                Ok(result) => listener.success(result),
                Err(e) => listener.error(e),
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

    /// Transfers with no transaction open, then commits in a transaction of
    /// its own
    async fn transfer_and_commit(
        request: LocalSyncRequest,
        options: SyncOptions,
        sync_svc: Arc<dyn SyncService>,
        catalog: dill::Catalog,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        match sync_svc
            .transfer_into_local(request, options, listener)
            .await?
        {
            SyncTransfer::UpToDate => Ok(SyncResult::UpToDate),
            SyncTransfer::Transferred(transferred) => {
                Self::commit_sync_transactionally(catalog, transferred).await
            }
        }
    }

    #[transactional_static_method1(local_sync_committer: Arc<dyn LocalSyncCommitter>)]
    async fn commit_sync_transactionally(
        catalog: dill::Catalog,
        transferred: TransferredSync,
    ) -> Result<SyncResult, SyncError> {
        local_sync_committer.commit(transferred).await
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

/// Services a transform job needs, cloned into each spawned job
#[derive(Clone)]
struct TransformServices {
    executor: Arc<dyn TransformExecutor>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
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
