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
use kamu::infra::DatasetKind;
use opendatafabric::*;
use url::Url;

use std::backtrace::BacktraceStatus;
use std::error::Error;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

type GenericPullResult = Result<Vec<(DatasetRefBuf, Result<PullResult, PullError>)>, CLIError>;

pub struct PullCommand {
    pull_svc: Arc<dyn PullService>,
    metadata_repo: Arc<dyn MetadataRepository>,
    output_config: Arc<OutputConfig>,
    refs: Vec<String>,
    all: bool,
    recursive: bool,
    force_uncacheable: bool,
    as_id: Option<String>,
    fetch: Option<String>,
}

impl PullCommand {
    pub fn new<I, S>(
        pull_svc: Arc<dyn PullService>,
        metadata_repo: Arc<dyn MetadataRepository>,
        output_config: Arc<OutputConfig>,
        refs: I,
        all: bool,
        recursive: bool,
        force_uncacheable: bool,
        as_id: Option<&str>,
        fetch: Option<&str>,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            pull_svc,
            metadata_repo,
            output_config,
            refs: refs.map(|s| s.as_ref().to_owned()).collect(),
            all: all,
            recursive,
            force_uncacheable,
            as_id: as_id.map(|s| s.to_owned()),
            fetch: fetch.map(|s| s.to_owned()),
        }
    }

    fn sync_from(&self, listener: Option<Arc<PrettyPullProgress>>) -> GenericPullResult {
        let as_id = self.as_id.as_ref().unwrap();
        let local_id = DatasetID::try_from(as_id).unwrap();
        let remote_ref = DatasetRef::try_from(&self.refs[0]).unwrap();

        let res = self.pull_svc.sync_from(
            remote_ref,
            local_id,
            PullOptions {
                create_remote_aliases: true,
                ..PullOptions::default()
            },
            listener.and_then(|p| p.begin_sync(local_id, remote_ref)),
        );

        Ok(vec![(remote_ref.into(), res)])
    }

    fn ingest_from(&self, listener: Option<Arc<PrettyPullProgress>>) -> GenericPullResult {
        let dataset_id = DatasetID::try_from(&self.refs[0]).unwrap();
        let summary = self.metadata_repo.get_summary(dataset_id)?;
        if summary.kind != DatasetKind::Root {
            return Err(CLIError::usage_error(
                "Cannot ingest data into non-root dataset",
            ));
        }

        let aliases = self.metadata_repo.get_remote_aliases(dataset_id)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.as_str())
            .collect();

        if !pull_aliases.is_empty() {
            return Err(CLIError::usage_error(format!(
                    "Ingesting data into remote dataset will cause histories to diverge. Existing pull aliases:\n{}",
                    pull_aliases.join("\n- ")
                )
            ));
        }

        let fetch_str = self.fetch.as_ref().unwrap();
        let path = Path::new(fetch_str);
        let url = if path.is_file() {
            Url::from_file_path(path.canonicalize().unwrap()).unwrap()
        } else {
            Url::parse(fetch_str).map_err(|e| {
                CLIError::usage_error(format!(
                    "Invalid fetch source, should be URL or path: {}",
                    e
                ))
            })?
        };

        let fetch_step = FetchStep::Url(FetchStepUrl {
            url: url.to_string(),
            event_time: None,
            cache: None,
        });

        let res = self.pull_svc.ingest_from(
            dataset_id,
            fetch_step,
            PullOptions {
                ingest_options: IngestOptions {
                    force_uncacheable: self.force_uncacheable,
                    exhaust_sources: true,
                },
                ..PullOptions::default()
            },
            listener.and_then(|p| p.begin_ingest(dataset_id)),
        );

        Ok(vec![(dataset_id.into(), res)])
    }

    fn pull_multi(&self, listener: Option<Arc<PrettyPullProgress>>) -> GenericPullResult {
        Ok(self.pull_svc.pull_multi(
            &mut self.refs.iter().map(|s| DatasetRef::try_from(s).unwrap()),
            PullOptions {
                recursive: self.recursive,
                all: self.all,
                create_remote_aliases: true,
                ingest_options: IngestOptions {
                    force_uncacheable: self.force_uncacheable,
                    exhaust_sources: true,
                },
                sync_options: SyncOptions::default(),
            },
            listener.clone().map(|v| v as Arc<dyn IngestMultiListener>),
            listener
                .clone()
                .map(|v| v as Arc<dyn TransformMultiListener>),
            listener.map(|v| v as Arc<dyn SyncMultiListener>),
        ))
    }

    fn pull_with_progress(&self) -> GenericPullResult {
        let pull_progress = PrettyPullProgress::new();
        let listener = Arc::new(pull_progress.clone());

        let draw_thread = std::thread::spawn(move || {
            pull_progress.draw();
        });

        let results = self.pull(Some(listener.clone()));

        listener.finish();
        draw_thread.join().unwrap();

        results
    }

    fn pull(&self, listener: Option<Arc<PrettyPullProgress>>) -> GenericPullResult {
        if self.as_id.is_some() {
            self.sync_from(listener)
        } else if self.fetch.is_some() {
            self.ingest_from(listener)
        } else {
            self.pull_multi(listener)
        }
    }
}

impl Command for PullCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        match (self.recursive, self.all, &self.as_id, &self.fetch) {
            (false, false, _, _) if self.refs.is_empty() => {
                Err(CLIError::usage_error("Specify a dataset or pass --all"))
            }
            (false, true, None, None) if self.refs.is_empty() => Ok(()),
            (_, false, None, None) if !self.refs.is_empty() => Ok(()),
            (false, false, Some(_), None) if self.refs.len() == 1 => Ok(()),
            (false, false, None, Some(_)) if self.refs.len() == 1 => Ok(()),
            _ => Err(CLIError::usage_error(
                "Invalid combination of arguments".to_owned(),
            )),
        }?;

        let pull_results = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            self.pull_with_progress()?
        } else {
            self.pull(None)?
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for (_, res) in pull_results.iter() {
            match res {
                Ok(r) => match r {
                    PullResult::UpToDate => up_to_date += 1,
                    PullResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if updated != 0 {
            eprintln!(
                "{}",
                console::style(format!("{} dataset(s) updated", updated))
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
            pull_results
                .into_iter()
                .filter_map(|(id, res)| res.err().map(|e| (id, e)))
                .for_each(|(id, err)| {
                    eprintln!(
                        "\n{}: {}",
                        console::style(format!("{}", id)).red().bold(),
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

        if errors != 0 {
            Err(CLIError::PartialFailure)
        } else {
            Ok(())
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Progress listeners / Visualizers
///////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct PrettyPullProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl PrettyPullProgress {
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

impl IngestMultiListener for PrettyPullProgress {
    fn begin_ingest(&self, dataset_id: &DatasetID) -> Option<Arc<dyn IngestListener>> {
        Some(Arc::new(PrettyIngestProgress::new(
            dataset_id,
            self.multi_progress.clone(),
        )))
    }
}

impl TransformMultiListener for PrettyPullProgress {
    fn begin_transform(&self, dataset_id: &DatasetID) -> Option<Arc<dyn TransformListener>> {
        Some(Arc::new(PrettyTransformProgress::new(
            dataset_id,
            self.multi_progress.clone(),
        )))
    }
}

impl SyncMultiListener for PrettyPullProgress {
    fn begin_sync(
        &self,
        local_dataset_id: &DatasetID,
        remote_dataset_ref: &DatasetRef,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            local_dataset_id.to_owned(),
            remote_dataset_ref.to_owned(),
            self.multi_progress.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ProgressStyle {
    Spinner,
    Bar,
}

struct PrettyIngestProgress {
    dataset_id: DatasetIDBuf,
    multi_progress: Arc<indicatif::MultiProgress>,
    state: Mutex<PrettyIngestProgressState>,
}

struct PrettyIngestProgressState {
    stage: IngestStage,
    curr_progress: indicatif::ProgressBar,
    curr_progress_style: ProgressStyle,
}

impl PrettyIngestProgress {
    fn new(dataset_id: &DatasetID, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            state: Mutex::new(PrettyIngestProgressState {
                stage: IngestStage::CheckCache,
                curr_progress_style: ProgressStyle::Spinner,
                curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                    dataset_id,
                    0,
                    "Checking for updates",
                ))),
            }),
            multi_progress: multi_progress,
        }
    }

    fn new_progress_bar(
        prefix: &str,
        pos: u64,
        len: u64,
        draw_delta: Option<u64>,
    ) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        pb.set_style(
            indicatif::ProgressStyle::default_bar()
            .template("{spinner:.cyan} Downloading {prefix:.white.bold}:\n  [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .progress_chars("#>-"));
        pb.set_prefix(prefix.to_owned());
        pb.set_length(len);
        pb.set_position(pos);
        if let Some(d) = draw_delta {
            pb.set_draw_delta(d);
        }
        pb
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        pb.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(100);
        pb
    }

    fn spinner_message<T: std::fmt::Display>(dataset_id: &DatasetID, step: u32, msg: T) -> String {
        let step_str = format!("[{}/7]", step + 1);
        let dataset = format!("({})", dataset_id);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }

    fn style_for_stage(&self, stage: IngestStage) -> ProgressStyle {
        match stage {
            IngestStage::Fetch => ProgressStyle::Bar,
            _ => ProgressStyle::Spinner,
        }
    }

    fn message_for_stage(&self, stage: IngestStage) -> String {
        let msg = match stage {
            IngestStage::CheckCache => "Checking for updates",
            IngestStage::Fetch => "Downloading data",
            IngestStage::Prepare => "Preparing data",
            IngestStage::Read => "Reading data",
            IngestStage::Preprocess => "Preprocessing data",
            IngestStage::Merge => "Merging data",
            IngestStage::Commit => "Committing data",
        };
        Self::spinner_message(&self.dataset_id, stage as u32, msg)
    }
}

impl IngestListener for PrettyIngestProgress {
    fn on_stage_progress(&self, stage: IngestStage, n: u64, out_of: u64) {
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
                    &self.dataset_id,
                    n,
                    out_of,
                    Some(1024),
                )),
            }
        } else {
            state
                .curr_progress
                .set_message(self.message_for_stage(stage));
            if state.curr_progress_style == ProgressStyle::Bar {
                state.curr_progress.set_position(n)
            }
        }
    }

    fn success(&self, result: &IngestResult) {
        let mut state = self.state.lock().unwrap();

        match result {
            IngestResult::UpToDate { uncacheable } => {
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_id,
                        IngestStage::Commit as u32,
                        if *uncacheable {
                            console::style(
                                "Dataset is uncachable (use --force-uncacheable to update)"
                                    .to_owned(),
                            )
                            .yellow()
                        } else {
                            console::style("Dataset is up-to-date".to_owned()).yellow()
                        },
                    ));
            }
            IngestResult::Updated {
                old_head: _,
                ref new_head,
                num_blocks: _,
                has_more: _,
                uncacheable,
            } => {
                if *uncacheable {
                    state
                        .curr_progress
                        .finish_with_message(Self::spinner_message(
                            &self.dataset_id,
                            IngestStage::Commit as u32,
                            console::style("Data source is uncacheable").yellow(),
                        ));
                    state.curr_progress = self.multi_progress.add(Self::new_spinner(""));
                };
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_id,
                        IngestStage::Commit as u32,
                        console::style(format!("Committed new block {}", new_head.short())).green(),
                    ));
            }
        };
    }

    fn error(&self, _error: &IngestError) {
        let state = self.state.lock().unwrap();
        state
            .curr_progress
            .finish_with_message(Self::spinner_message(
                &self.dataset_id,
                state.stage as u32,
                console::style("Failed to update root dataset").red(),
            ));
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
            &self.dataset_id,
            IngestStage::Read as u32,
            format!("Pulling engine image {}", image),
        ));
    }

    fn success(&self) {
        // Careful not to deadlock
        let stage = {
            let state = self.state.lock().unwrap();
            state.curr_progress.finish();
            state.stage
        };
        self.on_stage_progress(stage, 0, 0);
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettyTransformProgress {
    dataset_id: DatasetIDBuf,
    multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: Mutex<indicatif::ProgressBar>,
}

impl PrettyTransformProgress {
    fn new(dataset_id: &DatasetID, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            curr_progress: Mutex::new(multi_progress.add(Self::new_spinner(
                &Self::spinner_message(dataset_id, 0, "Applying derivative transformations"),
            ))),
            multi_progress: multi_progress,
        }
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        pb.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(100);
        pb
    }

    fn spinner_message<T: std::fmt::Display>(dataset_id: &DatasetID, step: u32, msg: T) -> String {
        let step_str = format!("[{}/1]", step + 1);
        let dataset = format!("({})", dataset_id);
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
            TransformResult::Updated {
                old_head: _,
                ref new_head,
                num_blocks: _,
            } => console::style(format!("Committed new block {}", new_head.short())).green(),
        };
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(&self.dataset_id, 0, msg));
    }

    fn error(&self, _error: &TransformError) {
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(
                &self.dataset_id,
                0,
                console::style("Failed to update derivative dataset").red(),
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
                &self.dataset_id,
                0,
                format!("Pulling engine image {}", image),
            ));
    }

    fn success(&self) {
        let mut curr_progress = self.curr_progress.lock().unwrap();
        curr_progress.finish();
        *curr_progress = self
            .multi_progress
            .add(Self::new_spinner(&Self::spinner_message(
                &self.dataset_id,
                0,
                "Applying derivative transformations",
            )));
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettySyncProgress {
    local_id: DatasetIDBuf,
    remote_ref: DatasetRefBuf,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
}

impl PrettySyncProgress {
    fn new(
        local_id: DatasetIDBuf,
        remote_ref: DatasetRefBuf,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        let inst = Self {
            local_id,
            remote_ref,
            curr_progress: multi_progress.add(Self::new_spinner("")),
            _multi_progress: multi_progress,
        };
        inst.curr_progress
            .set_message(inst.spinner_message(0, "Syncing remote dataset"));
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
        let dataset = format!("({} > {})", self.remote_ref, self.local_id);
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
            SyncResult::UpToDate => console::style("Dataset is up-to-date".to_owned()).yellow(),
            SyncResult::Updated {
                old_head: _,
                ref new_head,
                num_blocks,
            } => console::style(format!(
                "Updated to {} ({} block(s))",
                new_head.short(),
                num_blocks
            ))
            .green(),
        };
        self.curr_progress
            .finish_with_message(self.spinner_message(0, msg));
    }

    fn error(&self, _error: &SyncError) {
        self.curr_progress.finish_with_message(
            self.spinner_message(0, console::style("Failed to sync remote dataset").red()),
        );
    }
}
