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
use url::Url;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct PullCommand {
    pull_svc: Arc<dyn PullService>,
    local_repo: Arc<dyn LocalDatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRefAny>,
    all: bool,
    recursive: bool,
    fetch_uncacheable: bool,
    as_name: Option<DatasetName>,
    add_aliases: bool,
    fetch: Option<String>,
    force: bool,
}

impl PullCommand {
    pub fn new<I, R, S, SS>(
        pull_svc: Arc<dyn PullService>,
        local_repo: Arc<dyn LocalDatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        output_config: Arc<OutputConfig>,
        refs: I,
        all: bool,
        recursive: bool,
        fetch_uncacheable: bool,
        as_name: Option<S>,
        add_aliases: bool,
        fetch: Option<SS>,
        force: bool,
    ) -> Self
    where
        I: IntoIterator<Item = R>,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
        S: TryInto<DatasetName>,
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
        SS: Into<String>,
    {
        Self {
            pull_svc,
            local_repo,
            remote_alias_reg,
            output_config,
            refs: refs.into_iter().map(|s| s.try_into().unwrap()).collect(),
            all: all,
            recursive,
            fetch_uncacheable,
            as_name: as_name.map(|s| s.try_into().unwrap()),
            add_aliases,
            fetch: fetch.map(|s| s.into()),
            force,
        }
    }

    async fn sync_from(
        &self,
        listener: Option<Arc<PrettyPullProgress>>,
    ) -> Result<Vec<PullResponse>, CLIError> {
        let local_name = self.as_name.as_ref().unwrap();
        let remote_ref = self.refs[0].as_remote_ref().ok_or_else(|| {
            CLIError::usage_error("When using --as reference should point to a remote dataset")
        })?;

        Ok(self
            .pull_svc
            .pull_multi_ext(
                &mut vec![PullRequest {
                    local_ref: Some(local_name.into()),
                    remote_ref: Some(remote_ref),
                    ingest_from: None,
                }]
                .into_iter(),
                PullOptions {
                    add_aliases: self.add_aliases,
                    ..PullOptions::default()
                },
                None,
                None,
                listener.map(|v| v as Arc<dyn SyncMultiListener>),
            )
            .await?)
    }

    async fn ingest_from(
        &self,
        listener: Option<Arc<PrettyPullProgress>>,
    ) -> Result<Vec<PullResponse>, CLIError> {
        let dataset_ref = self.refs[0].as_local_ref().ok_or_else(|| {
            CLIError::usage_error("When using --fetch reference should point to a local dataset")
        })?;

        let dataset_handle = self.local_repo.resolve_dataset_ref(&dataset_ref).await?;

        let summary = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?
            .get_summary(SummaryOptions::default())
            .await?;

        if summary.kind != DatasetKind::Root {
            return Err(CLIError::usage_error(
                "Cannot ingest data into non-root dataset",
            ));
        }

        let aliases = self
            .remote_alias_reg
            .get_remote_aliases(&dataset_handle.as_local_ref())
            .await
            .map_err(CLIError::failure)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.to_string())
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

        Ok(self
            .pull_svc
            .pull_multi_ext(
                &mut vec![PullRequest {
                    local_ref: Some(dataset_ref),
                    remote_ref: None,
                    ingest_from: Some(fetch_step),
                }]
                .into_iter(),
                PullOptions {
                    ingest_options: IngestOptions {
                        fetch_uncacheable: self.fetch_uncacheable,
                        exhaust_sources: true,
                    },
                    ..PullOptions::default()
                },
                listener.map(|v| v as Arc<dyn IngestMultiListener>),
                None,
                None,
            )
            .await?)
    }

    async fn pull_multi(
        &self,
        listener: Option<Arc<PrettyPullProgress>>,
    ) -> Result<Vec<PullResponse>, CLIError> {
        Ok(self
            .pull_svc
            .pull_multi(
                &mut self.refs.iter().cloned(),
                PullOptions {
                    recursive: self.recursive,
                    all: self.all,
                    add_aliases: self.add_aliases,
                    ingest_options: IngestOptions {
                        fetch_uncacheable: self.fetch_uncacheable,
                        exhaust_sources: true,
                    },
                    sync_options: SyncOptions {
                        force: self.force,
                        ..SyncOptions::default()
                    },
                },
                listener.clone().map(|v| v as Arc<dyn IngestMultiListener>),
                listener
                    .clone()
                    .map(|v| v as Arc<dyn TransformMultiListener>),
                listener.map(|v| v as Arc<dyn SyncMultiListener>),
            )
            .await?)
    }

    async fn pull_with_progress(&self) -> Result<Vec<PullResponse>, CLIError> {
        let pull_progress = PrettyPullProgress::new();
        let listener = Arc::new(pull_progress.clone());

        let draw_thread = std::thread::spawn(move || {
            pull_progress.draw();
        });

        let res = self.pull(Some(listener.clone())).await;

        listener.finish();
        draw_thread.join().unwrap();

        res
    }

    async fn pull(
        &self,
        listener: Option<Arc<PrettyPullProgress>>,
    ) -> Result<Vec<PullResponse>, CLIError> {
        if self.as_name.is_some() {
            self.sync_from(listener).await
        } else if self.fetch.is_some() {
            self.ingest_from(listener).await
        } else {
            self.pull_multi(listener).await
        }
    }

    fn describe_response(&self, pr: &PullResponse) -> String {
        let local_ref = pr.local_ref.as_ref().or(pr
            .original_request
            .as_ref()
            .and_then(|r| r.local_ref.as_ref()));
        let remote_ref = pr.remote_ref.as_ref().or(pr
            .original_request
            .as_ref()
            .and_then(|r| r.remote_ref.as_ref()));
        match (
            local_ref,
            remote_ref,
            pr.original_request
                .as_ref()
                .and_then(|r| r.ingest_from.as_ref()),
        ) {
            (Some(local_ref), _, Some(_)) => {
                format!("ingest data into {} from custom source", local_ref)
            }
            (Some(local_ref), Some(remote_ref), _) => {
                format!("sync {} from {}", local_ref, remote_ref)
            }
            (None, Some(remote_ref), _) => format!("sync dataset from {}", remote_ref),
            (Some(local_ref), None, _) => format!("pull {}", local_ref),
            _ => format!("???"),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for PullCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match (self.recursive, self.all, &self.as_name, &self.fetch) {
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
            self.pull_with_progress().await?
        } else {
            self.pull(None).await?
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for res in pull_results.iter() {
            match &res.result {
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
            Err(BatchError::new(
                format!("Failed to update {} dataset(s)", errors),
                pull_results
                    .into_iter()
                    .filter(|res| res.result.is_err())
                    .map(|res| {
                        let ctx = format!("Failed to {}", self.describe_response(&res));
                        (res.result.err().unwrap(), ctx)
                    }),
            )
            .into())
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
    fn begin_ingest(&self, dataset_handle: &DatasetHandle) -> Option<Arc<dyn IngestListener>> {
        Some(Arc::new(PrettyIngestProgress::new(
            dataset_handle,
            self.multi_progress.clone(),
        )))
    }
}

impl TransformMultiListener for PrettyPullProgress {
    fn begin_transform(
        &self,
        dataset_handle: &DatasetHandle,
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
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        Some(Arc::new(PrettySyncProgress::new(
            dst.as_local_ref().unwrap(),
            src.as_remote_ref().unwrap(),
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
    dataset_handle: DatasetHandle,
    multi_progress: Arc<indicatif::MultiProgress>,
    state: Mutex<PrettyIngestProgressState>,
}

struct PrettyIngestProgressState {
    stage: IngestStage,
    curr_progress: indicatif::ProgressBar,
    curr_progress_style: ProgressStyle,
}

impl PrettyIngestProgress {
    fn new(dataset_handle: &DatasetHandle, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            state: Mutex::new(PrettyIngestProgressState {
                stage: IngestStage::CheckCache,
                curr_progress_style: ProgressStyle::Spinner,
                curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                    dataset_handle,
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

    fn spinner_message<T: std::fmt::Display>(
        dataset_handle: &DatasetHandle,
        step: u32,
        msg: T,
    ) -> String {
        let step_str = format!("[{}/7]", step + 1);
        let dataset = format!("({})", dataset_handle.name);
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
        Self::spinner_message(&self.dataset_handle, stage as u32, msg)
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
                    &self.dataset_handle.name,
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
            IngestResult::UpToDate {
                uncacheable,
                has_more,
            } => {
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
                        IngestStage::Commit as u32,
                        if *uncacheable {
                            console::style(
                                "Dataset is uncachable (use --fetch-uncacheable to update)"
                                    .to_owned(),
                            )
                            .yellow()
                        } else if !has_more {
                            console::style("Dataset is up-to-date".to_owned()).yellow()
                        } else {
                            console::style("No new data ingested".to_owned()).yellow()
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
                            &self.dataset_handle,
                            IngestStage::Commit as u32,
                            console::style("Data source is uncacheable").yellow(),
                        ));
                    state.curr_progress = self.multi_progress.add(Self::new_spinner(""));
                };
                state
                    .curr_progress
                    .finish_with_message(Self::spinner_message(
                        &self.dataset_handle,
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
            IngestStage::Read as u32,
            format!("Waiting for engine {}", engine_id),
        ));
    }

    fn success(&self) {
        self.on_stage_progress(IngestStage::Read, 0, 0);
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
            IngestStage::Read as u32,
            format!("Pulling image {}", image),
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
    dataset_handle: DatasetHandle,
    multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: Mutex<indicatif::ProgressBar>,
}

impl PrettyTransformProgress {
    fn new(dataset_handle: &DatasetHandle, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            curr_progress: Mutex::new(multi_progress.add(Self::new_spinner(
                &Self::spinner_message(dataset_handle, 0, "Applying derivative transformations"),
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

    fn spinner_message<T: std::fmt::Display>(
        dataset_handle: &DatasetHandle,
        step: u32,
        msg: T,
    ) -> String {
        let step_str = format!("[{}/1]", step + 1);
        let dataset = format!("({})", dataset_handle.name);
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
            .finish_with_message(Self::spinner_message(&self.dataset_handle, 0, msg));
    }

    fn error(&self, _error: &TransformError) {
        self.curr_progress
            .lock()
            .unwrap()
            .finish_with_message(Self::spinner_message(
                &self.dataset_handle,
                0,
                console::style("Failed to update derivative dataset").red(),
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
                format!("Waiting for engine {}", engine_id),
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
                format!("Pulling engine image {}", image),
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
        let dataset = format!("({} > {})", self.remote_ref, self.local_ref);
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
