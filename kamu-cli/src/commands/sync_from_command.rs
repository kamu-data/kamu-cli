use super::{Command, Error};
use crate::output::OutputConfig;
use kamu::domain::*;
use opendatafabric::*;

use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct SyncFromCommand {
    sync_svc: Arc<dyn SyncService>,
    ids: Vec<String>,
    remote: Option<String>,
    output_config: OutputConfig,
}

impl SyncFromCommand {
    pub fn new<I, S, S2>(
        sync_svc: Arc<dyn SyncService>,
        ids: I,
        remote: Option<S2>,
        output_config: &OutputConfig,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            sync_svc: sync_svc,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            remote: remote.map(|v| v.as_ref().to_owned()),
            output_config: output_config.clone(),
        }
    }

    fn pull(
        &self,
        dataset_ids: Vec<DatasetIDBuf>,
    ) -> Vec<(DatasetIDBuf, Result<SyncResult, SyncError>)> {
        dataset_ids
            .into_iter()
            .map(|id| {
                let result = self.sync_svc.sync_from(
                    &id,
                    &id,
                    RemoteID::try_from(self.remote.as_ref().unwrap()).unwrap(),
                    SyncOptions::default(),
                    None,
                );
                (id, result)
            })
            .collect()
    }
}

impl Command for SyncFromCommand {
    fn run(&mut self) -> Result<(), Error> {
        if self.ids.len() == 0 {
            return Err(Error::UsageError {
                msg: "Specify a dataset or pass --all".to_owned(),
            });
        }

        let dataset_ids: Vec<DatasetIDBuf> = self.ids.iter().map(|s| s.parse().unwrap()).collect();

        let spinner = if self.output_config.verbosity_level == 0 {
            let s = indicatif::ProgressBar::new_spinner();
            s.set_style(
                indicatif::ProgressStyle::default_spinner().template("{spinner:.cyan} {msg}"),
            );
            s.set_message("Pulling dataset(s) from a remote");
            s.enable_steady_tick(100);
            Some(s)
        } else {
            None
        };

        let mut updated = 0;
        let mut up_to_date = 0;
        let mut errors = 0;

        let results = self.pull(dataset_ids);

        for (_, res) in results.iter() {
            match res {
                Ok(r) => match r {
                    SyncResult::UpToDate => up_to_date += 1,
                    SyncResult::Updated { .. } => updated += 1,
                },
                Err(_) => errors += 1,
            }
        }

        if let Some(s) = spinner {
            s.finish_and_clear()
        }

        if updated != 0 {
            eprintln!(
                "{}",
                console::style(format!("{} dataset(s) pulled", updated))
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
