// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;
use kamu::domain::*;
use opendatafabric::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
    pub fn new<I, R, RR>(
        push_svc: Arc<dyn PushService>,
        refs: I,
        all: bool,
        recursive: bool,
        add_aliases: bool,
        force: bool,
        to: Option<RR>,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: Iterator<Item = R>,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
        RR: TryInto<DatasetRefRemote>,
        <RR as TryInto<DatasetRefRemote>>::Error: std::fmt::Debug,
    {
        Self {
            push_svc,
            refs: refs.map(|s| s.try_into().unwrap()).collect(),
            all,
            recursive,
            add_aliases,
            force,
            to: to.map(|s| s.try_into().unwrap()),
            output_config,
        }
    }

    async fn do_push(&self, listener: Option<Arc<dyn SyncMultiListener>>) -> Vec<PushResponse> {
        if let Some(remote_ref) = &self.to {
            self.push_svc
                .push_multi_ext(
                    &mut vec![PushRequest {
                        local_ref: self.refs[0].as_local_ref(),
                        remote_ref: Some(remote_ref.clone()),
                    }]
                    .into_iter(),
                    PushOptions {
                        all: self.all,
                        recursive: self.recursive,
                        add_aliases: self.add_aliases,
                        force: self.force,
                        sync_options: SyncOptions::default(),
                    },
                    listener,
                )
                .await
        } else {
            self.push_svc
                .push_multi(
                    &mut self.refs.iter().cloned(),
                    PushOptions {
                        all: self.all,
                        recursive: self.recursive,
                        add_aliases: self.add_aliases,
                        force: self.force,
                        sync_options: SyncOptions::default(),
                    },
                    listener,
                )
                .await
        }
    }

    async fn push_with_progress(&self) -> Vec<PushResponse> {
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
        };

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
            self.multi_progress.join().unwrap();
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
            src.as_local_ref().unwrap(),
            dst.as_remote_ref().unwrap(),
            self.multi_progress.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettySyncProgress {
    local_ref: DatasetRefLocal,
    remote_ref: DatasetRefRemote,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
}

impl PrettySyncProgress {
    fn new(
        local_ref: DatasetRefLocal,
        remote_ref: DatasetRefRemote,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        let inst = Self {
            local_ref,
            remote_ref,
            curr_progress: multi_progress.add(Self::new_spinner("")),
            _multi_progress: multi_progress,
        };
        inst.curr_progress
            .set_message(inst.spinner_message(0, "Syncing dataset to remote"));
        inst
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        pb.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(100);
        pb
    }

    fn spinner_message<T: std::fmt::Display>(&self, step: u32, msg: T) -> String {
        let step_str = format!("[{}/1]", step + 1);
        let dataset = format!("({} > {})", self.local_ref, self.remote_ref);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }
}

impl SyncListener for PrettySyncProgress {
    fn success(&self, result: &SyncResult) {
        let msg = match result {
            SyncResult::UpToDate => console::style("Remote is up-to-date".to_owned()).yellow(),
            SyncResult::Updated {
                old_head: _,
                ref new_head,
                num_blocks,
            } => console::style(format!(
                "Updated remote to {} ({} block(s))",
                new_head.short(),
                num_blocks
            ))
            .green(),
        };
        self.curr_progress
            .finish_with_message(self.spinner_message(0, msg));
    }

    fn error(&self, _error: &SyncError) {
        self.curr_progress.finish_with_message(self.spinner_message(
            0,
            console::style("Failed to sync dataset to repository").red(),
        ));
    }
}
