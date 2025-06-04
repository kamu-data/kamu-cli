// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use kamu::domain::auth::DatasetActionNotEnoughPermissionsError;
use kamu::domain::*;
use kamu::utils::datasets_filtering::filter_datasets_by_any_pattern;
use kamu_accounts::CurrentAccountSubject;

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct PullCommand {
    output_config: Arc<OutputConfig>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    search_svc: Arc<dyn SearchServiceRemote>,
    pull_dataset_use_case: Arc<dyn PullDatasetUseCase>,
    tenancy_config: TenancyConfig,
    current_account_subject: Arc<CurrentAccountSubject>,

    #[dill::component(explicit)]
    refs: Vec<odf::DatasetRefAnyPattern>,

    #[dill::component(explicit)]
    all: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    fetch_uncacheable: bool,

    #[dill::component(explicit)]
    as_name: Option<odf::DatasetName>,

    #[dill::component(explicit)]
    add_aliases: bool,

    #[dill::component(explicit)]
    force: bool,

    #[dill::component(explicit)]
    reset_derivatives_on_diverged_input: bool,

    #[dill::component(explicit)]
    new_dataset_visibility: odf::DatasetVisibility,
}

impl PullCommand {
    async fn sync_from(
        &self,
        listener: Option<Arc<dyn PullMultiListener>>,
        local_name: &odf::DatasetName,
    ) -> Result<Vec<PullResponse>, CLIError> {
        let remote_ref = match self.refs[0].as_dataset_ref_any() {
            Some(dataset_ref_any) => dataset_ref_any.as_remote_ref(|_| true).map_err(|_| {
                CLIError::usage_error("When using --as reference should point to a remote dataset")
            })?,
            None => {
                return Err(CLIError::usage_error(
                    "When using --as reference should not point to wildcard pattern",
                ));
            }
        };

        self.pull_dataset_use_case
            .execute_multi(
                vec![PullRequest::remote(
                    remote_ref,
                    Some(odf::DatasetAlias::new(
                        match self.tenancy_config {
                            TenancyConfig::MultiTenant => {
                                Some(self.current_account_subject.account_name().clone())
                            }
                            TenancyConfig::SingleTenant => None,
                        },
                        local_name.clone(),
                    )),
                )],
                PullOptions {
                    add_aliases: self.add_aliases,
                    sync_options: SyncOptions {
                        dataset_visibility: self.new_dataset_visibility,
                        force: self.force,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                listener,
            )
            .await
            .map_err(CLIError::failure)
    }

    async fn pull_multi(
        &self,
        listener: Option<Arc<dyn PullMultiListener>>,
        current_account_name: &odf::AccountName,
    ) -> Result<Vec<PullResponse>, CLIError> {
        let dataset_any_refs: Vec<_> = if !self.all {
            filter_datasets_by_any_pattern(
                self.dataset_registry.as_ref(),
                self.search_svc.clone(),
                self.refs.clone(),
                current_account_name,
                self.tenancy_config,
            )
            .try_collect()
            .await?
        } else {
            vec![]
        };

        let options = PullOptions {
            recursive: self.recursive,
            add_aliases: self.add_aliases,
            ingest_options: PollingIngestOptions {
                fetch_uncacheable: self.fetch_uncacheable,
                exhaust_sources: true,
                dataset_env_vars: HashMap::new(),
                schema_inference: SchemaInferenceOpts::default(),
            },
            transform_options: TransformOptions {
                reset_derivatives_on_diverged_input: self.reset_derivatives_on_diverged_input,
            },
            sync_options: SyncOptions {
                force: self.force,
                dataset_visibility: self.new_dataset_visibility,
                ..SyncOptions::default()
            },
        };

        if self.all {
            self.pull_dataset_use_case
                .execute_all_owned(options, listener)
                .await
                .map_err(CLIError::failure)
        } else {
            let requests = dataset_any_refs
                .into_iter()
                .map(|r| {
                    PullRequest::from_any_ref(&r, |_| {
                        self.tenancy_config == TenancyConfig::SingleTenant
                    })
                })
                .collect();

            self.pull_dataset_use_case
                .execute_multi(requests, options, listener)
                .await
                .map_err(CLIError::failure)
        }
    }

    async fn pull_with_progress(&self) -> Result<Vec<PullResponse>, CLIError> {
        let pull_progress = PrettyPullProgress::new(self.fetch_uncacheable, self.tenancy_config);
        let listener = Arc::new(pull_progress.clone());
        self.pull(Some(listener)).await
    }

    async fn pull(
        &self,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, CLIError> {
        if let Some(local_name) = &self.as_name {
            self.sync_from(listener, local_name).await
        } else {
            let current_account_name = self.current_account_subject.account_name();
            self.pull_multi(listener, current_account_name).await
        }
    }

    fn describe_response(&self, pr: &PullResponse) -> String {
        let local_ref = pr.maybe_local_ref.as_ref().map(Cow::Borrowed).or(pr
            .maybe_original_request
            .as_ref()
            .and_then(PullRequest::local_ref));
        let remote_ref = pr.maybe_remote_ref.as_ref().or(pr
            .maybe_original_request
            .as_ref()
            .and_then(|r| r.remote_ref()));
        match (local_ref, remote_ref) {
            (Some(local_ref), Some(remote_ref)) => {
                format!("sync {local_ref} from {remote_ref}")
            }
            (None, Some(remote_ref)) => format!("sync dataset from {remote_ref}"),
            (Some(local_ref), None) => format!("pull {local_ref}"),
            _ => "???".to_string(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for PullCommand {
    #[allow(clippy::match_same_arms)]
    async fn run(&self) -> Result<(), CLIError> {
        match (self.refs.len(), self.recursive, self.all, &self.as_name) {
            (0, _, false, _) => Err(CLIError::usage_error("Specify a dataset or pass --all")),
            (0, false, true, None) => Ok(()),
            (1, false, false, Some(_)) if self.refs.len() == 1 => Ok(()),
            (1, false, false, None) if self.refs.len() == 1 => Ok(()),
            (refs, _, false, None) if refs > 0 => Ok(()),
            _ => Err(CLIError::usage_error(
                "Invalid combination of arguments".to_owned(),
            )),
        }?;

        let pull_results = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            self.pull_with_progress().await?
        } else {
            self.pull(None).await?
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for res in &pull_results {
            match &res.result {
                Ok(r) => match r {
                    PullResult::UpToDate(_) => up_to_date += 1,
                    PullResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if updated != 0 && !self.output_config.quiet {
            eprintln!(
                "{}",
                console::style(format!("{updated} dataset(s) updated"))
                    .green()
                    .bold()
            );
        }
        if up_to_date != 0 && !self.output_config.quiet {
            eprintln!(
                "{}",
                console::style(format!("{up_to_date} dataset(s) up-to-date"))
                    .yellow()
                    .bold()
            );
        }
        if errors != 0 {
            Err(BatchError::new(
                format!("Failed to update {errors} dataset(s)"),
                pull_results
                    .into_iter()
                    .filter(|res| res.result.is_err())
                    .map(|res| {
                        let ctx = format!("Failed to {}", self.describe_response(&res));
                        let err = res.result.err().map(sanitize_pull_error).unwrap();

                        (err, ctx)
                    }),
            )
            .into())
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Progress listeners / Visualizers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct PrettyPullProgress {
    multi_progress: Arc<indicatif::MultiProgress>,
    fetch_uncacheable: bool,
    tenancy_config: TenancyConfig,
}

impl PrettyPullProgress {
    fn new(fetch_uncacheable: bool, tenancy_config: TenancyConfig) -> Self {
        Self {
            multi_progress: Arc::new(indicatif::MultiProgress::new()),
            fetch_uncacheable,
            tenancy_config,
        }
    }
}

impl PullMultiListener for PrettyPullProgress {
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

impl PollingIngestMultiListener for PrettyPullProgress {
    fn begin_ingest(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Option<Arc<dyn PollingIngestListener>> {
        Some(Arc::new(PrettyIngestProgress::new(
            dataset_handle,
            self.multi_progress.clone(),
            self.fetch_uncacheable,
        )))
    }
}

impl TransformMultiListener for PrettyPullProgress {
    fn begin_transform(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Option<Arc<dyn TransformListener>> {
        Some(Arc::new(PrettyTransformProgress::new(
            dataset_handle,
            self.multi_progress.clone(),
        )))
    }
}

impl SyncMultiListener for PrettyPullProgress {
    fn begin_sync(
        &self,
        src: &odf::DatasetRefAny,
        dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            dst.as_local_ref(|_| self.tenancy_config == TenancyConfig::SingleTenant)
                .expect("Expected local ref"),
            src.as_remote_ref(|_| true).expect("Expected remote ref"),
            self.multi_progress.clone(),
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ProgressStyle {
    Spinner,
    Bar,
}

pub struct PrettyIngestProgress {
    dataset_handle: odf::DatasetHandle,
    multi_progress: Arc<indicatif::MultiProgress>,
    fetch_uncacheable: bool,
    state: Mutex<PrettyIngestProgressState>,
}

struct PrettyIngestProgressState {
    stage: PollingIngestStage,
    curr_progress: indicatif::ProgressBar,
    curr_progress_style: ProgressStyle,
}

impl PrettyIngestProgress {
    pub fn new(
        dataset_handle: &odf::DatasetHandle,
        multi_progress: Arc<indicatif::MultiProgress>,
        fetch_uncacheable: bool,
    ) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            state: Mutex::new(PrettyIngestProgressState {
                stage: PollingIngestStage::CheckCache,
                curr_progress_style: ProgressStyle::Spinner,
                curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                    dataset_handle,
                    0,
                    "Checking for updates",
                ))),
            }),
            multi_progress,
            fetch_uncacheable,
        }
    }

    fn new_progress_bar(prefix: &str, pos: u64, len: u64) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_bar()
            .template(
                "{spinner:.cyan} Downloading {prefix:.white.bold}:\n  [{elapsed_precise}] \
                 [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
            )
            .unwrap()
            .progress_chars("#>-");
        pb.set_style(style);
        pb.set_prefix(prefix.to_owned());
        pb.set_length(len);
        pb.set_position(pos);
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        pb.set_style(style);
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn spinner_message<T: std::fmt::Display>(
        dataset_handle: &odf::DatasetHandle,
        step: u32,
        msg: T,
    ) -> String {
        let step_str = format!("[{}/7]", step + 1);
        let dataset = format!("({})", dataset_handle.alias);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }

    fn style_for_stage(&self, stage: PollingIngestStage) -> ProgressStyle {
        match stage {
            PollingIngestStage::Fetch => ProgressStyle::Bar,
            _ => ProgressStyle::Spinner,
        }
    }

    fn message_for_stage(&self, stage: PollingIngestStage) -> String {
        let msg = match stage {
            PollingIngestStage::CheckCache => "Checking for updates",
            PollingIngestStage::Fetch => "Downloading data",
            PollingIngestStage::Prepare => "Preparing data",
            PollingIngestStage::Read => "Reading data",
            PollingIngestStage::Preprocess => "Preprocessing data",
            PollingIngestStage::Merge => "Merging data",
            PollingIngestStage::Commit => "Committing data",
        };
        Self::spinner_message(&self.dataset_handle, stage as u32, msg)
    }
}

impl PollingIngestListener for PrettyIngestProgress {
    fn on_cache_hit(&self, created_at: &DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();

        state
            .curr_progress
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                PollingIngestStage::Fetch as u32,
                console::style(format!(
                    "Using cached data from {} (use kamu system gc to clear cache)",
                    chrono_humanize::HumanTime::from(*created_at - Utc::now())
                ))
                .yellow(),
            ));
        state.curr_progress = self.multi_progress.add(Self::new_spinner(""));
    }

    fn on_stage_progress(&self, stage: PollingIngestStage, n: u64, out_of: TotalSteps) {
        let mut state = self.state.lock().unwrap();
        state.stage = stage;

        if state.curr_progress.is_finished()
            || state.curr_progress_style != self.style_for_stage(stage)
        {
            state.curr_progress.finish();
            state.curr_progress_style = self.style_for_stage(stage);
            state.curr_progress = match state.curr_progress_style {
                ProgressStyle::Spinner => self
                    .multi_progress
                    .add(Self::new_spinner(&self.message_for_stage(stage))),
                ProgressStyle::Bar => self.multi_progress.add(Self::new_progress_bar(
                    &self.dataset_handle.alias.to_string(),
                    n,
                    match out_of {
                        TotalSteps::Unknown => n,
                        TotalSteps::Exact(t) => t,
                    },
                )),
            }
        } else {
            state
                .curr_progress
                .set_message(self.message_for_stage(stage));
            if state.curr_progress_style == ProgressStyle::Bar {
                state.curr_progress.set_position(n);
                state.curr_progress.set_length(match out_of {
                    TotalSteps::Unknown => n,
                    TotalSteps::Exact(t) => t,
                });
            }
        }
    }

    fn success(&self, result: &PollingIngestResult) {
        let mut state = self.state.lock().unwrap();

        match result {
            PollingIngestResult::UpToDate {
                no_source_defined,
                uncacheable,
            } => {
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
                        PollingIngestStage::Commit as u32,
                        if *no_source_defined {
                            console::style("Dataset does not specify a polling source".to_owned())
                                .yellow()
                        } else if *uncacheable && !self.fetch_uncacheable {
                            console::style(
                                "Dataset is uncacheable (use --fetch-uncacheable to update)"
                                    .to_owned(),
                            )
                            .yellow()
                        } else {
                            console::style("Dataset is up-to-date".to_owned()).yellow()
                        },
                    ));
            }
            PollingIngestResult::Updated {
                new_head,
                uncacheable,
                ..
            } => {
                if *uncacheable {
                    state
                        .curr_progress
                        .finish_with_message(Self::spinner_message(
                            &self.dataset_handle,
                            PollingIngestStage::Commit as u32,
                            console::style("Data source is uncacheable").yellow(),
                        ));
                    state.curr_progress = self.multi_progress.add(Self::new_spinner(""));
                }
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
                        PollingIngestStage::Commit as u32,
                        console::style(format!(
                            "Committed new block {}",
                            new_head.as_multibase().short()
                        ))
                        .green(),
                    ));
            }
        }
    }

    fn error(&self, _error: &PollingIngestError) {
        let state = self.state.lock().unwrap();
        state
            .curr_progress
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                state.stage as u32,
                console::style("Failed to update root dataset").red(),
            ));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        Some(self)
    }
}

impl EngineProvisioningListener for PrettyIngestProgress {
    fn begin(&self, engine_id: &str) {
        let state = self.state.lock().unwrap();

        // This currently happens during the Read stage
        state.curr_progress.set_message(Self::spinner_message(
            &self.dataset_handle,
            PollingIngestStage::Read as u32,
            format!("Waiting for engine {engine_id}"),
        ));
    }

    fn success(&self) {
        self.on_stage_progress(PollingIngestStage::Read, 0, TotalSteps::Exact(0));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }
}

impl PullImageListener for PrettyIngestProgress {
    fn begin(&self, image: &str) {
        let state = self.state.lock().unwrap();

        // This currently happens during the Read stage
        state.curr_progress.set_message(Self::spinner_message(
            &self.dataset_handle,
            PollingIngestStage::Read as u32,
            format!("Pulling image {image}"),
        ));
    }

    fn success(&self) {
        // Careful not to deadlock
        let stage = {
            let state = self.state.lock().unwrap();
            state.curr_progress.finish();
            state.stage
        };
        self.on_stage_progress(stage, 0, TotalSteps::Exact(0));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PrettyTransformProgress {
    dataset_handle: odf::DatasetHandle,
    multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: Mutex<indicatif::ProgressBar>,
}

impl PrettyTransformProgress {
    fn new(
        dataset_handle: &odf::DatasetHandle,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            curr_progress: Mutex::new(multi_progress.add(Self::new_spinner(
                &Self::spinner_message(dataset_handle, 0, "Applying derivative transformations"),
            ))),
            multi_progress,
        }
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        pb.set_style(style);
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn spinner_message<T: std::fmt::Display>(
        dataset_handle: &odf::DatasetHandle,
        step: u32,
        msg: T,
    ) -> String {
        let step_str = format!("[{}/1]", step + 1);
        let dataset = format!("({})", dataset_handle.alias);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }
}

impl TransformListener for PrettyTransformProgress {
    fn success(&self, result: &TransformResult) {
        let msg = match result {
            TransformResult::UpToDate => {
                console::style("Dataset is up-to-date".to_owned()).yellow()
            }
            TransformResult::Updated { new_head, .. } => console::style(format!(
                "Committed new block {}",
                new_head.as_multibase().short()
            ))
            .green(),
        };
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(&self.dataset_handle, 0, msg));
    }

    fn elaborate_error(&self, _error: &TransformElaborateError) {
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                0,
                console::style("Failed to update derivative dataset (elaborate phase)").red(),
            ));
    }

    fn execute_error(&self, _error: &TransformExecuteError) {
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                0,
                console::style("Failed to update derivative dataset (execute phase)").red(),
            ));
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        Some(self)
    }
}

impl EngineProvisioningListener for PrettyTransformProgress {
    fn begin(&self, engine_id: &str) {
        self.curr_progress
            .lock()
            .unwrap()
            .set_message(Self::spinner_message(
                &self.dataset_handle,
                0,
                format!("Waiting for engine {engine_id}"),
            ));
    }

    fn success(&self) {
        let curr_progress = self.curr_progress.lock().unwrap();
        curr_progress.set_message(Self::spinner_message(
            &self.dataset_handle,
            0,
            "Applying derivative transformations",
        ));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }
}

impl PullImageListener for PrettyTransformProgress {
    fn begin(&self, image: &str) {
        self.curr_progress
            .lock()
            .unwrap()
            .set_message(Self::spinner_message(
                &self.dataset_handle,
                0,
                format!("Pulling engine image {image}"),
            ));
    }

    fn success(&self) {
        let mut curr_progress = self.curr_progress.lock().unwrap();
        curr_progress.finish();
        *curr_progress = self
            .multi_progress
            .add(Self::new_spinner(&Self::spinner_message(
                &self.dataset_handle,
                0,
                "Applying derivative transformations",
            )));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PrettySyncProgress {
    local_ref: odf::DatasetRef,
    remote_ref: odf::DatasetRefRemote,
    multi_progress: Arc<indicatif::MultiProgress>,
    state: Mutex<PrettySyncProgressState>,
}

struct PrettySyncProgressState {
    stage: SyncStage,
    progress: indicatif::ProgressBar,
}

impl PrettySyncProgress {
    fn new(
        local_ref: odf::DatasetRef,
        remote_ref: odf::DatasetRefRemote,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        Self {
            state: Mutex::new(PrettySyncProgressState {
                stage: SyncStage::CommitBlocks,
                progress: multi_progress.add(Self::new_spinner(&local_ref, &remote_ref)),
            }),
            local_ref,
            remote_ref,
            multi_progress,
        }
    }

    fn new_spinner(
        local_ref: &odf::DatasetRef,
        remote_ref: &odf::DatasetRefRemote,
    ) -> indicatif::ProgressBar {
        let spinner = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg} {prefix:.dim}")
            .unwrap();
        spinner.set_style(style);
        spinner.set_prefix(format!("({remote_ref} > {local_ref})"));
        spinner.set_message("Syncing remote dataset".to_owned());
        spinner.enable_steady_tick(Duration::from_millis(100));
        spinner
    }
}

impl SyncListener for PrettySyncProgress {
    fn on_status(&self, stage: SyncStage, stats: &SyncStats) {
        let mut state = self.state.lock().unwrap();

        if state.stage != stage {
            state.stage = stage;

            let pb = match stage {
                SyncStage::ReadMetadata => {
                    let pb = indicatif::ProgressBar::hidden();
                    let style = indicatif::ProgressStyle::default_bar()
                        .template(
                            "{spinner:.cyan} Syncing metadata {prefix:.dim}:\n  \
                             [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
                        )
                        .unwrap()
                        .progress_chars("#>-");
                    pb.set_style(style);
                    pb.set_prefix(format!("({} > {})", self.remote_ref, self.local_ref));
                    pb.set_length(stats.src_estimated.metadata_blocks_read);
                    pb.set_position(stats.src.metadata_blocks_read);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb
                }
                SyncStage::TransferData => {
                    let pb = indicatif::ProgressBar::hidden();
                    let style = indicatif::ProgressStyle::default_bar()
                        .template(
                            "{spinner:.cyan} Syncing data & checkpoints {prefix:.dim}:\n  \
                             [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} \
                             ({bytes_per_sec}, {eta})",
                        )
                        .unwrap()
                        .progress_chars("#>-");
                    pb.set_style(style);
                    pb.set_prefix(format!("({} > {})", self.remote_ref, self.local_ref));
                    pb.set_length(stats.src_estimated.bytes_read);
                    pb.set_position(stats.src.bytes_read);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb
                }
                SyncStage::CommitBlocks => {
                    let pb = indicatif::ProgressBar::hidden();
                    let style = indicatif::ProgressStyle::default_bar()
                        .template(
                            "{spinner:.cyan} Committing blocks {prefix:.dim}:\n  \
                             [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
                        )
                        .unwrap()
                        .progress_chars("#>-");
                    pb.set_style(style);
                    pb.set_prefix(format!("({} > {})", self.remote_ref, self.local_ref));
                    pb.set_length(stats.dst_estimated.metadata_blocks_written);
                    pb.set_position(stats.dst.metadata_blocks_written);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb
                }
            };

            state.progress = self.multi_progress.add(pb);
        }

        match stage {
            SyncStage::ReadMetadata => {
                state
                    .progress
                    .set_length(stats.src_estimated.metadata_blocks_read);
                state.progress.set_position(stats.src.metadata_blocks_read);
            }
            SyncStage::TransferData => {
                state.progress.set_length(stats.src_estimated.bytes_read);
                state.progress.set_position(stats.src.bytes_read);
            }
            SyncStage::CommitBlocks => {
                state
                    .progress
                    .set_length(stats.dst_estimated.metadata_blocks_written);
                state
                    .progress
                    .set_position(stats.dst.metadata_blocks_written);
            }
        }
    }

    fn success(&self, result: &SyncResult) {
        let msg = match result {
            SyncResult::UpToDate => console::style("Dataset is up-to-date".to_owned()).yellow(),
            SyncResult::Updated {
                new_head,
                num_blocks,
                ..
            } => console::style(format!(
                "Updated to {} ({} block(s))",
                new_head.as_multibase().short(),
                num_blocks
            ))
            .green(),
        };

        let mut state = self.state.lock().unwrap();

        state.progress = self
            .multi_progress
            .add(Self::new_spinner(&self.local_ref, &self.remote_ref));

        state.progress.finish_with_message(msg.to_string());
    }

    fn error(&self, _error: &SyncError) {
        let mut state = self.state.lock().unwrap();

        state.progress = self
            .multi_progress
            .add(Self::new_spinner(&self.local_ref, &self.remote_ref));

        state.progress.finish_with_message(
            console::style("Failed to sync remote dataset")
                .red()
                .to_string(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn sanitize_pull_error(original_pull_error: PullError) -> PullError {
    // Tricky way...
    if let PullError::Access(odf::AccessError::Unauthorized(forbidden_error)) = &original_pull_error
        && let Some(permissions_error) =
            forbidden_error.downcast_ref::<DatasetActionNotEnoughPermissionsError>()
    {
        let dataset_ref = permissions_error.dataset_ref.clone();
        return PullError::NotFound(odf::DatasetNotFoundError { dataset_ref });
    }

    // No miracle happened, giving away the error that was
    original_pull_error
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
