// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    backtrace::BacktraceStatus,
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::output::OutputConfig;

type GenericVerificationResult = Result<
    Vec<(
        DatasetRefLocal,
        Result<VerificationResult, VerificationError>,
    )>,
    CLIError,
>;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct VerifyCommand {
    dataset_reg: Arc<dyn DatasetRegistry>,
    verification_svc: Arc<dyn VerificationService>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRefLocal>,
    recursive: bool,
    integrity: bool,
}

impl VerifyCommand {
    pub fn new<I, R>(
        dataset_reg: Arc<dyn DatasetRegistry>,
        verification_svc: Arc<dyn VerificationService>,
        output_config: Arc<OutputConfig>,
        refs: I,
        recursive: bool,
        integrity: bool,
    ) -> Self
    where
        I: Iterator<Item = R>,
        R: TryInto<DatasetRefLocal>,
        <R as TryInto<DatasetRefLocal>>::Error: std::fmt::Debug,
    {
        Self {
            dataset_reg,
            verification_svc,
            output_config,
            refs: refs.map(|s| s.try_into().unwrap()).collect(),
            recursive,
            integrity,
        }
    }

    fn verify_with_progress(&self, options: VerificationOptions) -> GenericVerificationResult {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self.verify(options, Some(listener.clone()));

        listener.finish();
        draw_thread.join().unwrap();

        results
    }

    fn verify(
        &self,
        options: VerificationOptions,
        listener: Option<Arc<VerificationMultiProgress>>,
    ) -> GenericVerificationResult {
        let dataset_handle = self
            .dataset_reg
            .resolve_dataset_ref(self.refs.first().unwrap())?;

        let listener = listener.and_then(|l| l.begin_verify(&dataset_handle));

        let res = self.verification_svc.verify(
            &dataset_handle.as_local_ref(),
            (None, None),
            options,
            listener,
        );

        Ok(vec![(dataset_handle.into(), res)])
    }
}

impl Command for VerifyCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.refs.len() != 1 {
            return Err(CLIError::usage_error(
                "Verifying multiple datasets at once is not yet supported",
            ));
        }

        if self.recursive {
            return Err(CLIError::usage_error(
                "Verifying datasets recursively is not yet supported",
            ));
        }

        let options = if self.integrity {
            VerificationOptions {
                check_integrity: true,
                replay_transformations: false,
            }
        } else {
            VerificationOptions {
                check_integrity: true,
                replay_transformations: true,
            }
        };

        let verification_results = if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            self.verify_with_progress(options)?
        } else {
            self.verify(options, None)?
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
    fn begin_verify(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Option<Arc<dyn VerificationListener>> {
        Some(Arc::new(VerificationProgress::new(
            dataset_handle,
            self.multi_progress.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////

struct VerificationProgress {
    dataset_handle: DatasetHandle,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
    state: Mutex<VerificationState>,
}

struct VerificationState {
    block_hash: Option<Multihash>,
    block_index: usize,
    num_blocks: usize,
    phase: VerificationPhase,
}

impl VerificationProgress {
    fn new(dataset_handle: &DatasetHandle, multi_progress: Arc<indicatif::MultiProgress>) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            curr_progress: multi_progress.add(Self::new_spinner("Initializing")),
            _multi_progress: multi_progress,
            state: Mutex::new(VerificationState {
                block_hash: None,
                block_index: 0,
                num_blocks: 0,
                phase: VerificationPhase::DataIntegrity,
            }),
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
        block: Option<&Multihash>,
    ) -> String {
        let step_str = if out_of != 0 {
            format!("[{}/{}]", step, out_of)
        } else {
            format!("[-/-]")
        };

        let dataset = if let Some(block) = block {
            format!("({} @ {})", self.dataset_handle.name, block.short())
        } else {
            format!("({})", self.dataset_handle.name)
        };

        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }

    fn save_state(
        &self,
        block_hash: &Multihash,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        let mut s = self.state.lock().unwrap();
        s.block_hash = Some(block_hash.clone());
        s.block_index = block_index;
        s.num_blocks = num_blocks;
        s.phase = phase;
    }
}

impl VerificationListener for VerificationProgress {
    fn begin(&self) {
        self.curr_progress
            .set_message(self.spinner_message(0, 0, "Verifying dataset", None))
    }

    fn success(&self, result: &VerificationResult) {
        match result {
            VerificationResult::Valid => {
                let s = self.state.lock().unwrap();
                self.curr_progress.finish_with_message(self.spinner_message(
                    s.num_blocks,
                    s.num_blocks,
                    console::style("Dataset is valid".to_owned()).green(),
                    None,
                ))
            }
        }
    }

    fn error(&self, error: &VerificationError) {
        let s = self.state.lock().unwrap();
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
            s.block_index + 1,
            s.num_blocks,
            console::style(msg).red(),
            None,
        ));
    }

    fn begin_phase(&self, _phase: VerificationPhase, _num_blocks: usize) {}
    fn end_phase(&self, _phase: VerificationPhase, _num_blocks: usize) {}

    fn begin_block(
        &self,
        block_hash: &Multihash,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        self.save_state(block_hash, block_index, num_blocks, phase);
        match phase {
            VerificationPhase::DataIntegrity => {
                self.curr_progress.set_message(self.spinner_message(
                    block_index + 1,
                    num_blocks,
                    "Verifying data integrity",
                    Some(block_hash),
                ))
            }
            VerificationPhase::ReplayTransform => {
                self.curr_progress.set_message(self.spinner_message(
                    block_index + 1,
                    num_blocks,
                    "Replaying transformation",
                    Some(block_hash),
                ))
            }
        }
    }

    fn end_block(
        &self,
        _block_hash: &Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        Some(self)
    }
}

impl TransformListener for VerificationProgress {
    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        Some(self)
    }
}

impl EngineProvisioningListener for VerificationProgress {
    fn begin(&self, engine_id: &str) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            format!("Waiting for engine {}", engine_id),
            s.block_hash.as_ref(),
        ))
    }

    fn success(&self) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            "Replaying transformation",
            s.block_hash.as_ref(),
        ))
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }
}

impl PullImageListener for VerificationProgress {
    fn begin(&self, image: &str) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            format!("Pulling engine image {}", image),
            s.block_hash.as_ref(),
        ))
    }

    fn success(&self) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            "Replaying transformation",
            s.block_hash.as_ref(),
        ))
    }
}
