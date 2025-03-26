// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use console::style as s;
use futures::TryStreamExt;
use kamu::domain::*;
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Command
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct PushCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    push_dataset_use_case: Arc<dyn PushDatasetUseCase>,

    #[dill::component(explicit)]
    refs: Vec<odf::DatasetRefPattern>,

    #[dill::component(explicit)]
    all: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    add_aliases: bool,

    #[dill::component(explicit)]
    force: bool,

    #[dill::component(explicit)]
    to: Option<odf::DatasetPushTarget>,

    #[dill::component(explicit)]
    new_dataset_visibility: Option<odf::DatasetVisibility>,

    #[dill::component(explicit)]
    output_config: Arc<OutputConfig>,
}

impl PushCommand {
    async fn do_push(
        &self,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PushResponse>, CLIError> {
        let dataset_refs: Vec<_> =
            filter_datasets_by_local_pattern(self.dataset_registry.as_ref(), self.refs.clone())
                .map_ok(|dataset_handle| dataset_handle.as_local_ref())
                .try_collect()
                .await?;

        let mut dataset_handles = Vec::new();
        let mut error_responses = Vec::new();
        // TODO: batch resolution
        for dataset_ref in &dataset_refs {
            match self
                .dataset_registry
                .resolve_dataset_handle_by_ref(dataset_ref)
                .await
            {
                Ok(hdl) => dataset_handles.push(hdl),
                Err(e) => {
                    let push_error = match e {
                        odf::DatasetRefUnresolvedError::NotFound(e) => PushError::SourceNotFound(e),
                        odf::DatasetRefUnresolvedError::Internal(e) => PushError::Internal(e),
                    };
                    error_responses.push(PushResponse {
                        local_handle: None,
                        target: None,
                        result: Err(push_error),
                    });
                }
            }
        }

        if !error_responses.is_empty() {
            return Ok(error_responses);
        }

        self.push_dataset_use_case
            .execute_multi(
                dataset_handles,
                PushMultiOptions {
                    all: self.all,
                    recursive: self.recursive,
                    add_aliases: self.add_aliases,
                    sync_options: self.sync_options(),
                    remote_target: self.to.clone(),
                },
                listener,
            )
            .await
            .map_err(CLIError::failure)
    }

    fn sync_options(&self) -> SyncOptions {
        SyncOptions {
            force: self.force,
            dataset_visibility: self
                .new_dataset_visibility
                .unwrap_or(odf::DatasetVisibility::Private),
            ..SyncOptions::default()
        }
    }

    async fn push_with_progress(&self) -> Result<Vec<PushResponse>, CLIError> {
        let progress = PrettyPushProgress::new(self.tenancy_config);
        let listener = Arc::new(progress.clone());
        self.do_push(Some(listener.clone())).await
    }
}

#[async_trait::async_trait(?Send)]
impl Command for PushCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.refs.is_empty() && !self.all {
            return Err(CLIError::usage_error("Specify a dataset or pass --all"));
        }

        if self.refs.len() > 1 && self.to.is_some() {
            return Err(CLIError::usage_error(
                "Cannot specify multiple datasets when using --to",
            ));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let push_results = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            self.push_with_progress().await
        } else {
            self.do_push(None).await
        }?;

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for res in &push_results {
            match &res.result {
                Ok(sync_result) => match sync_result {
                    SyncResult::UpToDate => up_to_date += 1,
                    SyncResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if !self.output_config.quiet {
            if updated != 0 {
                eprintln!(
                    "{}",
                    s(format!("{updated} dataset(s) pushed")).green().bold()
                );
            }
            if up_to_date != 0 {
                if self.new_dataset_visibility.is_some() {
                    eprintln!(
                        "{}",
                        s("Dataset(s) have already been pushed -- the visibility marker ignored")
                            .yellow()
                            .bold()
                    );
                }

                eprintln!(
                    "{}",
                    s(format!("{up_to_date} dataset(s) up-to-date"))
                        .yellow()
                        .bold()
                );
            }
        }

        if errors != 0 {
            Err(BatchError::new(
                format!("Failed to push {errors} dataset(s)"),
                push_results
                    .into_iter()
                    .filter(|res| res.result.is_err())
                    .map(|res| {
                        let push_err = format!("Failed to push {res}");
                        let err = res.result.err().unwrap();
                        (err, push_err)
                    }),
            )
            .into())
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct PrettyPushProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub tenancy_config: TenancyConfig,
}

impl PrettyPushProgress {
    fn new(tenancy_config: TenancyConfig) -> Self {
        Self {
            multi_progress: Arc::new(indicatif::MultiProgress::new()),
            tenancy_config,
        }
    }
}

impl SyncMultiListener for PrettyPushProgress {
    fn begin_sync(
        &self,
        src: &odf::DatasetRefAny,
        dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            src.as_local_ref(|_| self.tenancy_config == TenancyConfig::SingleTenant)
                .expect("Expected local ref"),
            dst.as_remote_ref(|_| true).expect("Expected remote ref"),
            self.multi_progress.clone(),
        )))
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
        spinner.set_prefix(format!("({local_ref} > {remote_ref})"));
        spinner.set_message("Syncing dataset to repository".to_owned());
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
                            "{spinner:.cyan} Analyzing metadata {prefix:.dim}:\n  \
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
                    pb.set_length(stats.dst_estimated.bytes_written);
                    pb.set_position(stats.dst.bytes_written);
                    pb.enable_steady_tick(Duration::from_millis(100));
                    pb
                }
                SyncStage::CommitBlocks => {
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
                state.progress.set_length(stats.dst_estimated.bytes_written);
                state.progress.set_position(stats.dst.bytes_written);
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
            SyncResult::UpToDate => s("Repository is up-to-date".to_owned()).yellow(),
            SyncResult::Updated {
                ref new_head,
                num_blocks,
                ..
            } => s(format!(
                "Updated repository to {} ({} block(s))",
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

        state
            .progress
            .finish_with_message(s("Failed to sync dataset to repository").red().to_string());
    }
}
