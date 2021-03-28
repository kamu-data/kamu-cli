use super::{Command, Error};
use crate::output::OutputConfig;
use kamu::domain::*;
use opendatafabric::*;

use std::backtrace::BacktraceStatus;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct PullCommand {
    pull_svc: Rc<RefCell<dyn PullService>>,
    ids: Vec<String>,
    all: bool,
    recursive: bool,
    force_uncacheable: bool,
    output_config: OutputConfig,
}

impl PullCommand {
    pub fn new<I, S>(
        pull_svc: Rc<RefCell<dyn PullService>>,
        ids: I,
        all: bool,
        recursive: bool,
        force_uncacheable: bool,
        output_config: &OutputConfig,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            pull_svc: pull_svc,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            all: all,
            recursive: recursive,
            force_uncacheable: force_uncacheable,
            output_config: output_config.clone(),
        }
    }

    fn pull_quiet(
        &self,
        dataset_ids: Vec<DatasetIDBuf>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        self.pull_svc.borrow_mut().pull_multi(
            &mut dataset_ids.iter().map(|id| id.as_ref()),
            PullOptions {
                recursive: self.recursive,
                all: self.all,
                ingest_options: IngestOptions {
                    force_uncacheable: self.force_uncacheable,
                    exhaust_sources: true,
                },
            },
            None,
            None,
        )
    }

    fn pull_with_progress(
        &self,
        dataset_ids: Vec<DatasetIDBuf>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let pull_progress = PrettyPullProgress::new();
        let listener = Arc::new(Mutex::new(pull_progress.clone()));

        let draw_thread = std::thread::spawn(move || {
            pull_progress.draw();
        });

        let results = self.pull_svc.borrow_mut().pull_multi(
            &mut dataset_ids.iter().map(|id| id.as_ref()),
            PullOptions {
                recursive: self.recursive,
                all: self.all,
                ingest_options: IngestOptions {
                    force_uncacheable: self.force_uncacheable,
                    exhaust_sources: true,
                },
            },
            Some(listener.clone()),
            Some(listener.clone()),
        );

        listener.lock().unwrap().finish();
        draw_thread.join().unwrap();

        results
    }
}

impl Command for PullCommand {
    fn run(&mut self) -> Result<(), Error> {
        let dataset_ids: Vec<DatasetIDBuf> = match (&self.ids[..], self.recursive, self.all) {
            ([], false, false) => {
                return Err(Error::UsageError {
                    msg: "Specify a dataset or pass --all".to_owned(),
                })
            }
            ([], false, true) => vec![],
            (ref ids, _, false) => ids.iter().map(|s| s.parse().unwrap()).collect(),
            _ => {
                return Err(Error::UsageError {
                    msg: "Invalid combination of arguments".to_owned(),
                })
            }
        };

        let results = if self.output_config.verbosity_level == 0 {
            self.pull_with_progress(dataset_ids)
        } else {
            self.pull_quiet(dataset_ids)
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        for (_, res) in results.iter() {
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
            results
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

        Ok(())
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
    fn begin_ingest(&mut self, dataset_id: &DatasetID) -> Option<Arc<Mutex<dyn IngestListener>>> {
        Some(Arc::new(Mutex::new(PrettyIngestProgress::new(
            dataset_id,
            self.multi_progress.clone(),
        ))))
    }
}

impl TransformMultiListener for PrettyPullProgress {
    fn begin_transform(
        &mut self,
        dataset_id: &DatasetID,
    ) -> Option<Arc<Mutex<dyn TransformListener>>> {
        Some(Arc::new(Mutex::new(PrettyTransformProgress::new(
            dataset_id,
            self.multi_progress.clone(),
        ))))
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
    stage: IngestStage,
    multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
    curr_progress_style: ProgressStyle,
}

impl PrettyIngestProgress {
    fn new(dataset_id: &DatasetID, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            stage: IngestStage::CheckCache,
            curr_progress_style: ProgressStyle::Spinner,
            curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                dataset_id,
                0,
                "Checking for updates",
            ))),
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
        pb.set_prefix(prefix);
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
        pb.set_message(msg);
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
    fn on_stage_progress(&mut self, stage: IngestStage, n: u64, out_of: u64) {
        self.stage = stage;

        if self.curr_progress.is_finished()
            || self.curr_progress_style != self.style_for_stage(stage)
        {
            self.curr_progress.finish();
            self.curr_progress_style = self.style_for_stage(stage);
            self.curr_progress = match self.curr_progress_style {
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
            self.curr_progress
                .set_message(&self.message_for_stage(stage));
            if self.curr_progress_style == ProgressStyle::Bar {
                self.curr_progress.set_position(n)
            }
        }
    }

    fn success(&mut self, result: &IngestResult) {
        match result {
            IngestResult::UpToDate { uncacheable } => {
                self.curr_progress
                    .finish_with_message(&Self::spinner_message(
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
                ref block_hash,
                has_more: _,
                uncacheable,
            } => {
                if *uncacheable {
                    self.curr_progress
                        .finish_with_message(&Self::spinner_message(
                            &self.dataset_id,
                            IngestStage::Commit as u32,
                            console::style("Data source is uncacheable").yellow(),
                        ));
                    self.curr_progress = self.multi_progress.add(Self::new_spinner(""));
                };
                self.curr_progress
                    .finish_with_message(&Self::spinner_message(
                        &self.dataset_id,
                        IngestStage::Commit as u32,
                        console::style(format!("Committed new block {}", block_hash.short()))
                            .green(),
                    ));
            }
        };
    }

    fn error(&mut self, _error: &IngestError) {
        self.curr_progress
            .finish_with_message(&Self::spinner_message(
                &self.dataset_id,
                self.stage as u32,
                console::style("Failed to update root dataset").red(),
            ));
    }

    fn get_pull_image_listener(&mut self) -> Option<&mut dyn PullImageListener> {
        Some(self)
    }
}

impl PullImageListener for PrettyIngestProgress {
    fn begin(&mut self, image: &str) {
        // This currently happens during the Read stage
        self.curr_progress.set_message(&Self::spinner_message(
            &self.dataset_id,
            IngestStage::Read as u32,
            format!("Pulling engine image {}", image),
        ));
    }

    fn success(&mut self) {
        self.curr_progress.finish();
        self.on_stage_progress(self.stage, 0, 0);
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettyTransformProgress {
    dataset_id: DatasetIDBuf,
    multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
}

impl PrettyTransformProgress {
    fn new(dataset_id: &DatasetID, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            curr_progress: multi_progress.add(Self::new_spinner(&Self::spinner_message(
                dataset_id,
                0,
                "Applying derivative transformations",
            ))),
            multi_progress: multi_progress,
        }
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        pb.set_style(indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"));
        pb.set_message(msg);
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
    fn success(&mut self, result: &TransformResult) {
        let msg = match result {
            TransformResult::UpToDate => {
                console::style("Dataset is up-to-date".to_owned()).yellow()
            }
            TransformResult::Updated { ref block_hash } => {
                console::style(format!("Committed new block {}", block_hash.short())).green()
            }
        };
        self.curr_progress
            .finish_with_message(&Self::spinner_message(&self.dataset_id, 0, msg));
    }

    fn error(&mut self, _error: &TransformError) {
        self.curr_progress
            .finish_with_message(&Self::spinner_message(
                &self.dataset_id,
                0,
                console::style("Failed to update derivative dataset").red(),
            ));
    }

    fn get_pull_image_listener(&mut self) -> Option<&mut dyn PullImageListener> {
        Some(self)
    }
}

impl PullImageListener for PrettyTransformProgress {
    fn begin(&mut self, image: &str) {
        self.curr_progress.set_message(&Self::spinner_message(
            &self.dataset_id,
            0,
            format!("Pulling engine image {}", image),
        ));
    }

    fn success(&mut self) {
        self.curr_progress.finish();
        self.curr_progress = self
            .multi_progress
            .add(Self::new_spinner(&Self::spinner_message(
                &self.dataset_id,
                0,
                "Applying derivative transformations",
            )));
    }
}
