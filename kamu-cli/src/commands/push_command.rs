// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kamu::domain::*;
use opendatafabric::*;

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct PushCommand {
    push_svc: Arc<dyn PushService>,
    refs: Vec<DatasetRefAny>,
    all: bool,
    recursive: bool,
    add_aliases: bool,
    force: bool,
    to: Option<DatasetRefRemote>,
    output_config: Arc<OutputConfig>,
}

impl PushCommand {
    pub fn new<I>(
        push_svc: Arc<dyn PushService>,
        refs: I,
        all: bool,
        recursive: bool,
        add_aliases: bool,
        force: bool,
        to: Option<DatasetRefRemote>,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: Iterator<Item = DatasetRefAny>,
    {
        Self {
            push_svc,
            refs: refs.collect(),
            all,
            recursive,
            add_aliases,
            force,
            to,
            output_config,
        }
    }

    async fn do_push(
        &self,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PushResponse>, CLIError> {
        if let Some(remote_ref) = &self.to {
            let local_ref = self.refs[0].as_local_single_tenant_ref().map_err(|_| {
                CLIError::usage_error("When using --to reference should point to a local dataset")
            })?;

            Ok(self
                .push_svc
                .push_multi_ext(
                    &mut vec![PushRequest {
                        local_ref: Some(local_ref),
                        remote_ref: Some(remote_ref.clone()),
                    }]
                    .into_iter(),
                    PushOptions {
                        all: self.all,
                        recursive: self.recursive,
                        add_aliases: self.add_aliases,
                        sync_options: self.sync_options(),
                    },
                    listener,
                )
                .await)
        } else {
            Ok(self
                .push_svc
                .push_multi(
                    &mut self.refs.iter().cloned(),
                    PushOptions {
                        all: self.all,
                        recursive: self.recursive,
                        add_aliases: self.add_aliases,
                        sync_options: self.sync_options(),
                    },
                    listener,
                )
                .await)
        }
    }

    fn sync_options(&self) -> SyncOptions {
        SyncOptions {
            force: self.force,
            ..SyncOptions::default()
        }
    }

    async fn push_with_progress(&self) -> Result<Vec<PushResponse>, CLIError> {
        let progress = PrettyPushProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self.do_push(Some(listener.clone())).await;

        listener.finish();
        draw_thread.join().unwrap();

        results
    }
}

#[async_trait::async_trait(?Send)]
impl Command for PushCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if self.refs.len() == 0 && !self.all {
            return Err(CLIError::usage_error("Specify a dataset or pass --all"));
        }

        if self.refs.len() > 1 && self.to.is_some() {
            return Err(CLIError::usage_error(
                "Cannot specify multiple datasets when using --to",
            ));
        }

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

        for res in push_results.iter() {
            match &res.result {
                Ok(r) => match r {
                    SyncResult::UpToDate => up_to_date += 1,
                    SyncResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if updated != 0 {
            eprintln!(
                "{}",
                console::style(format!("{} dataset(s) pushed", updated))
                    .green()
                    .bold()
            );
        }
        if up_to_date != 0 {
            eprintln!(
                "{}",
                console::style(format!("{} dataset(s) up-to-date", up_to_date))
                    .yellow()
                    .bold()
            );
        }
        if errors != 0 {
            Err(BatchError::new(
                format!("Failed to push {} dataset(s)", errors),
                push_results
                    .into_iter()
                    .filter(|res| res.result.is_err())
                    .map(|res| {
                        (
                            res.result.err().unwrap(),
                            format!("Failed to push {}", res.original_request),
                        )
                    }),
            )
            .into())
        } else {
            Ok(())
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct PrettyPushProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl PrettyPushProgress {
    fn new() -> Self {
        Self {
            multi_progress: Arc::new(indicatif::MultiProgress::new()),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    fn draw(&self) {
        loop {
            if self.finished.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }
}

impl SyncMultiListener for PrettyPushProgress {
    fn begin_sync(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            src.as_local_ref(|_| true).expect("Expected local ref"),
            dst.as_remote_ref(|_| true).expect("Expected remote ref"),
            self.multi_progress.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettySyncProgress {
    local_ref: DatasetRef,
    remote_ref: DatasetRefRemote,
    multi_progress: Arc<indicatif::MultiProgress>,
    state: Mutex<PrettySyncProgressState>,
}

struct PrettySyncProgressState {
    stage: SyncStage,
    progress: indicatif::ProgressBar,
}

impl PrettySyncProgress {
    fn new(
        local_ref: DatasetRef,
        remote_ref: DatasetRefRemote,
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
        local_ref: &DatasetRef,
        remote_ref: &DatasetRefRemote,
    ) -> indicatif::ProgressBar {
        let spinner = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg} {prefix:.dim}")
            .unwrap();
        spinner.set_style(style);
        spinner.set_prefix(format!("({} > {})", local_ref, remote_ref));
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
                    pb.set_length(stats.src_estimated.metadata_blocks_read as u64);
                    pb.set_position(stats.src.metadata_blocks_read as u64);
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
                    pb.set_length(stats.dst_estimated.bytes_written as u64);
                    pb.set_position(stats.dst.bytes_written as u64);
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
                    pb.set_length(stats.dst_estimated.metadata_blocks_writen as u64);
                    pb.set_position(stats.dst.metadata_blocks_writen as u64);
                    pb
                }
            };

            state.progress = self.multi_progress.add(pb);
        }

        match stage {
            SyncStage::ReadMetadata => {
                state
                    .progress
                    .set_length(stats.src_estimated.metadata_blocks_read as u64);
                state
                    .progress
                    .set_position(stats.src.metadata_blocks_read as u64);
            }
            SyncStage::TransferData => {
                state
                    .progress
                    .set_length(stats.dst_estimated.bytes_written as u64);
                state.progress.set_position(stats.dst.bytes_written as u64);
            }
            SyncStage::CommitBlocks => {
                state
                    .progress
                    .set_length(stats.dst_estimated.metadata_blocks_writen as u64);
                state
                    .progress
                    .set_position(stats.dst.metadata_blocks_writen as u64);
            }
        }
    }

    fn success(&self, result: &SyncResult) {
        let msg = match result {
            SyncResult::UpToDate => console::style("Repository is up-to-date".to_owned()).yellow(),
            SyncResult::Updated {
                old_head: _,
                ref new_head,
                num_blocks,
            } => console::style(format!(
                "Updated repository to {} ({} block(s))",
                new_head.short(),
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
            console::style("Failed to sync dataset to repository")
                .red()
                .to_string(),
        );
    }
}
