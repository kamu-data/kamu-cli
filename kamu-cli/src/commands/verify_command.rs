// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use kamu::domain::*;
use opendatafabric::*;

use super::{BatchError, CLIError, Command};
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
    local_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRefLocal>,
    recursive: bool,
    integrity: bool,
}

impl VerifyCommand {
    pub fn new<I>(
        local_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        output_config: Arc<OutputConfig>,
        refs: I,
        recursive: bool,
        integrity: bool,
    ) -> Self
    where
        I: Iterator<Item = DatasetRefLocal>,
    {
        Self {
            local_repo,
            verification_svc,
            output_config,
            refs: refs.collect(),
            recursive,
            integrity,
        }
    }

    async fn verify_with_progress(
        &self,
        options: VerificationOptions,
    ) -> GenericVerificationResult {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let results = self.verify(options, Some(listener.clone())).await;

        listener.finish();
        draw_thread.join().unwrap();

        results
    }

    async fn verify(
        &self,
        options: VerificationOptions,
        listener: Option<Arc<VerificationMultiProgress>>,
    ) -> GenericVerificationResult {
        let dataset_handle = self
            .local_repo
            .resolve_dataset_ref(self.refs.first().unwrap())
            .await?;

        let listener = listener.and_then(|l| l.begin_verify(&dataset_handle));

        let res = self
            .verification_svc
            .verify(
                &dataset_handle.as_local_ref(),
                (None, None),
                options,
                listener,
            )
            .await;

        Ok(vec![(dataset_handle.into(), res)])
    }
}

#[async_trait::async_trait(?Send)]
impl Command for VerifyCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
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
            self.verify_with_progress(options).await?
        } else {
            self.verify(options, None).await?
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
            Err(BatchError::new(
                format!("Failed to verify {} dataset(s)", errors),
                verification_results.into_iter().filter_map(|(id, res)| {
                    res.err().map(|e| (e, format!("Failed to verify {}", id)))
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
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        pb.set_style(style);
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(Duration::from_millis(100));
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

    fn begin_phase(&self, phase: VerificationPhase) {
        let message = match phase {
            VerificationPhase::DataIntegrity => "Verifying data integrity",
            VerificationPhase::ReplayTransform => "Replaying transformations",
            VerificationPhase::MetadataIntegrity => "Verifying metadata integrity",
        };
        self.curr_progress.set_message(message);
    }

    fn end_phase(&self, _phase: VerificationPhase) {}

    fn begin_block(
        &self,
        block_hash: &Multihash,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        self.save_state(block_hash, block_index, num_blocks, phase);
        match phase {
            VerificationPhase::MetadataIntegrity => unreachable!(),
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
