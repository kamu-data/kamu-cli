use super::{Command, Error};
use crate::output::OutputConfig;
use kamu::domain::*;
use opendatafabric::*;

use std::backtrace::BacktraceStatus;
use std::convert::TryFrom;
use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct PushCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    push_svc: Arc<dyn PushService>,
    refs: Vec<String>,
    all: bool,
    recursive: bool,
    as_ref: Option<String>,
    output_config: OutputConfig,
}

impl PushCommand {
    pub fn new<I, S, S2>(
        metadata_repo: Arc<dyn MetadataRepository>,
        push_svc: Arc<dyn PushService>,
        refs: I,
        all: bool,
        recursive: bool,
        as_ref: Option<S2>,
        output_config: &OutputConfig,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            metadata_repo,
            push_svc,
            refs: refs.map(|s| s.as_ref().to_owned()).collect(),
            all,
            recursive,
            as_ref: as_ref.map(|s| s.as_ref().to_owned()),
            output_config: output_config.clone(),
        }
    }

    fn push_quiet(&self) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        self.push_svc.push_multi(
            &mut self.refs.iter().map(|s| DatasetRef::try_from(s).unwrap()),
            PushOptions {
                all: self.all,
                recursive: self.recursive,
                sync_options: SyncOptions::default(),
            },
            None,
        )
    }

    fn push_with_progress(&self) -> Vec<(PushInfo, Result<PushResult, PushError>)> {
        let progress = PrettyPushProgress::new();
        let listener = Arc::new(Mutex::new(progress.clone()));

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self.push_svc.push_multi(
            &mut self.refs.iter().map(|s| DatasetRef::try_from(s).unwrap()),
            PushOptions {
                all: self.all,
                recursive: self.recursive,
                sync_options: SyncOptions::default(),
            },
            Some(listener.clone()),
        );

        listener.lock().unwrap().finish();
        draw_thread.join().unwrap();

        results
    }
}

impl Command for PushCommand {
    fn run(&mut self) -> Result<(), Error> {
        if self.refs.len() == 0 && !self.all {
            return Err(Error::UsageError {
                msg: "Specify a dataset or pass --all".to_owned(),
            });
        }

        if self.refs.len() > 1 && self.as_ref.is_some() {
            return Err(Error::UsageError {
                msg: "Cannot specify multiple datasets with --as alias".to_owned(),
            });
        }

        // If --as alias is used - add it to push aliases
        if let Some(ref as_ref) = self.as_ref {
            let local_id = match DatasetRef::try_from(&self.refs[0]).unwrap().as_local() {
                Some(local_id) => local_id,
                None => {
                    return Err(Error::UsageError {
                        msg: "When using --as dataset has to refer to a local ID".to_owned(),
                    })
                }
            };
            let remote_ref = DatasetRefBuf::try_from(as_ref.clone()).unwrap();
            if remote_ref.is_local() {
                return Err(Error::UsageError {
                    msg: "When using --as the alias has to be a remote reference".to_owned(),
                });
            }

            self.metadata_repo
                .get_remote_aliases(local_id)?
                .add(remote_ref, RemoteAliasKind::Push)?;
        }

        let push_results = if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
            self.push_with_progress()
        } else {
            self.push_quiet()
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
        &mut self,
        local_dataset_id: &DatasetID,
        remote_dataset_ref: &DatasetRef,
    ) -> Option<Arc<Mutex<dyn SyncListener>>> {
        Some(Arc::new(Mutex::new(PrettySyncProgress::new(
            local_dataset_id.to_owned(),
            remote_dataset_ref.to_owned(),
            self.multi_progress.clone(),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct PrettySyncProgress {
    local_dataset_id: DatasetIDBuf,
    remote_dataset_ref: DatasetRefBuf,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
}

impl PrettySyncProgress {
    fn new(
        local_dataset_id: DatasetIDBuf,
        remote_dataset_ref: DatasetRefBuf,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        let inst = Self {
            local_dataset_id: local_dataset_id.clone(),
            remote_dataset_ref: remote_dataset_ref.clone(),
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
        let dataset = format!("({} > {})", self.local_dataset_id, self.remote_dataset_ref);
        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }
}

impl SyncListener for PrettySyncProgress {
    fn success(&mut self, result: &SyncResult) {
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

    fn error(&mut self, _error: &SyncError) {
        self.curr_progress.finish_with_message(
            self.spinner_message(0, console::style("Failed to sync dataset to remote").red()),
        );
    }
}
