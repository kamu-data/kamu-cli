// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use crate::output::OutputConfig;
use kamu::domain::*;
use opendatafabric::*;

use std::backtrace::BacktraceStatus;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct PushCommand {
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    push_svc: Arc<dyn PushService>,
    refs: Vec<DatasetRefAny>,
    all: bool,
    recursive: bool,
    as_name: Option<RemoteDatasetName>,
    output_config: Arc<OutputConfig>,
}

impl PushCommand {
    pub fn new<I, R, S>(
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        push_svc: Arc<dyn PushService>,
        refs: I,
        all: bool,
        recursive: bool,
        as_name: Option<S>,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: Iterator<Item = R>,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
        S: TryInto<RemoteDatasetName>,
        <S as TryInto<RemoteDatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            remote_alias_reg,
            push_svc,
            refs: refs.map(|s| s.try_into().unwrap()).collect(),
            all,
            recursive,
            as_name: as_name.map(|s| s.try_into().unwrap()),
            output_config,
        }
    }

    async fn push_quiet(&self) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        self.push_svc
            .push_multi(
                &mut self.refs.iter().cloned(),
                PushOptions {
                    all: self.all,
                    recursive: self.recursive,
                    sync_options: SyncOptions::default(),
                },
                None,
            )
            .await
    }

    async fn push_with_progress(&self) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        let progress = PrettyPushProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self
            .push_svc
            .push_multi(
                &mut self.refs.iter().cloned(),
                PushOptions {
                    all: self.all,
                    recursive: self.recursive,
                    sync_options: SyncOptions::default(),
                },
                Some(listener.clone()),
            )
            .await;

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

        if self.refs.len() > 1 && self.as_name.is_some() {
            return Err(CLIError::usage_error(
                "Cannot specify multiple datasets with --as alias",
            ));
        }

        // If --as alias is used - add it to push aliases
        let alias_added = if let Some(remote_name) = &self.as_name {
            let local_ref = match self.refs[0].as_local_ref() {
                Some(local_ref) => local_ref,
                None => {
                    return Err(CLIError::usage_error(
                        "When using --as dataset has to refer to a local ID",
                    ))
                }
            };

            self.remote_alias_reg
                .get_remote_aliases(&local_ref)?
                .add(remote_name, RemoteAliasKind::Push)?
        } else {
            false
        };

        let push_results = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            self.push_with_progress().await
        } else {
            self.push_quiet().await
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for (_, res) in push_results.iter() {
            match res {
                Ok(r) => match r {
                    PushResult::UpToDate => up_to_date += 1,
                    PushResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if alias_added && errors != 0 {
            // This is a bit ugly, but we don't want alias to stay unless the first push is successful
            self.remote_alias_reg
                .get_remote_aliases(&self.refs[0].as_local_ref().unwrap())?
                .delete(self.as_name.as_ref().unwrap(), RemoteAliasKind::Push)?;
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
            eprintln!(
                "{}\n\n{}:",
                console::style(format!("{} dataset(s) had errors", errors))
                    .red()
                    .bold(),
                console::style("Summary of errors")
            );

            push_results
                .into_iter()
                .filter_map(|(pi, res)| res.err().map(|e| (pi, e)))
                .for_each(|(pi, err)| {
                    eprintln!(
                        "\n{}: {}",
                        console::style(format!("{}", pi.original_ref)).red().bold(),
                        err
                    );
                    if let Some(bt) = err.backtrace() {
                        if bt.status() == BacktraceStatus::Captured {
                            eprintln!("{}", console::style(bt).dim());
                        }
                    }

                    let mut source = err.source();
                    while source.is_some() {
                        if let Some(bt) = source.unwrap().backtrace() {
                            if bt.status() == BacktraceStatus::Captured {
                                eprintln!("\nCaused by: {}", source.unwrap());
                                eprintln!("{}", console::style(bt).dim());
                            }
                        }
                        source = source.unwrap().source();
                    }
                });
        }

        Ok(())
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
        local_ref: &DatasetRefLocal,
        remote_ref: &DatasetRefRemote,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            local_ref.clone(),
            remote_ref.clone(),
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
