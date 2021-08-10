use std::{
    backtrace::BacktraceStatus,
    convert::TryFrom,
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use kamu::domain::*;
use opendatafabric::{DatasetID, DatasetIDBuf, Sha3_256};

use super::{CLIError, Command};
use crate::output::OutputConfig;

type GenericVerificationResult =
    Result<Vec<(DatasetIDBuf, Result<VerificationResult, VerificationError>)>, CLIError>;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct VerifyCommand {
    transform_svc: Arc<dyn TransformService>,
    output_config: Arc<OutputConfig>,
    ids: Vec<String>,
    recursive: bool,
}

impl VerifyCommand {
    pub fn new<I, S>(
        transform_svc: Arc<dyn TransformService>,
        output_config: Arc<OutputConfig>,
        ids: I,
        recursive: bool,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            transform_svc,
            output_config,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            recursive,
        }
    }

    fn verify_with_progress(&self) -> GenericVerificationResult {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self.verify(Some(listener.clone()));

        listener.finish();
        draw_thread.join().unwrap();

        results
    }

    fn verify(
        &self,
        listener: Option<Arc<VerificationMultiProgress>>,
    ) -> GenericVerificationResult {
        let dataset_id = self
            .ids
            .first()
            .map(|s| DatasetIDBuf::try_from(s.to_owned()).unwrap())
            .unwrap();

        let listener = listener.and_then(|l| l.begin_verify(&dataset_id));

        let res = self.transform_svc.verify(
            &dataset_id,
            (None, None),
            VerificationOptions::default(),
            listener,
        );

        Ok(vec![(dataset_id, res)])
    }
}

impl Command for VerifyCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.ids.len() != 1 {
            return Err(CLIError::usage_error(
                "Verifying multiple datasets at once is not yet supported",
            ));
        }

        if self.recursive {
            return Err(CLIError::usage_error(
                "Verifying datasets recursively is not yet supported",
            ));
        }

        let verification_results =
            if self.output_config.is_tty && self.output_config.verbosity_level == 0 {
                self.verify_with_progress()?
            } else {
                self.verify(None)?
            };

        let mut valid = 0;
        let mut errors = 0;

        for (_, res) in verification_results.iter() {
            match res {
                Ok(_) => valid += 1,
                Err(_) => errors += 1,
            }
        }

        if valid != 0 {
            eprintln!(
                "{}",
                console::style(format!("{} dataset(s) are valid", valid))
                    .green()
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
            verification_results
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
struct VerificationMultiProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl VerificationMultiProgress {
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

impl VerificationMultiListener for VerificationMultiProgress {
    fn begin_verify(&self, dataset_id: &DatasetID) -> Option<Arc<dyn VerificationListener>> {
        Some(Arc::new(VerificationProgress::new(
            dataset_id,
            self.multi_progress.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct VerificationProgress {
    dataset_id: DatasetIDBuf,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
}

impl VerificationProgress {
    fn new(dataset_id: &DatasetID, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_id: dataset_id.to_owned(),
            curr_progress: multi_progress.add(Self::new_spinner("Initializing")),
            _multi_progress: multi_progress,
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
        &self,
        step: usize,
        out_of: usize,
        msg: T,
        block: Option<&Sha3_256>,
    ) -> String {
        let step_str = if out_of != 0 {
            format!("[{}/{}]", step + 1, out_of)
        } else {
            format!("[-/-]")
        };

        let dataset = if let Some(block) = block {
            format!("({} @ {})", self.dataset_id, block.short())
        } else {
            format!("({})", self.dataset_id)
        };

        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }
}

impl VerificationListener for VerificationProgress {
    fn begin(&self, num_blocks: usize) {
        self.curr_progress.set_message(self.spinner_message(
            0,
            num_blocks,
            "Verifying dataset",
            None,
        ))
    }

    fn on_phase(
        &self,
        block: &Sha3_256,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        match phase {
            VerificationPhase::HashData => self.curr_progress.set_message(self.spinner_message(
                block_index,
                num_blocks,
                "Hashing data",
                Some(block),
            )),
            VerificationPhase::ReplayTransform => {
                self.curr_progress.set_message(self.spinner_message(
                    block_index,
                    num_blocks,
                    "Replaying transformation",
                    Some(block),
                ))
            }
            VerificationPhase::BlockValid => self.curr_progress.set_message(self.spinner_message(
                block_index,
                num_blocks,
                "Block valid",
                Some(block),
            )),
        }
    }

    fn success(&self, result: &VerificationResult) {
        match result {
            VerificationResult::Valid { blocks_verified } => {
                self.curr_progress.finish_with_message(self.spinner_message(
                    blocks_verified - 1,
                    *blocks_verified,
                    console::style("Dataset is valid".to_owned()).green(),
                    None,
                ))
            }
        }
    }

    fn error(&self, error: &VerificationError) {
        let msg = match error {
            VerificationError::DataDoesNotMatchMetadata(..) => {
                format!("Validation error (data doesn't match metadata)")
            }
            VerificationError::DataNotReproducible(..) => {
                format!("Validation error (data is not reproducible)")
            }
            _ => format!("Error during transformation"),
        };
        self.curr_progress.finish_with_message(self.spinner_message(
            0,
            0,
            console::style(msg).red(),
            None,
        ));
    }
}
