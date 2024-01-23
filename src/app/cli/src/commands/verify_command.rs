// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::*;

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;
use crate::VerificationMultiProgress;

type GenericVerificationResult =
    Result<Vec<(DatasetRef, Result<VerificationResult, VerificationError>)>, CLIError>;

///////////////////////////////////////////////////////////////////////////////

pub struct VerifyCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRef>,
    recursive: bool,
    integrity: bool,
}

impl VerifyCommand {
    pub fn new<I>(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        output_config: Arc<OutputConfig>,
        refs: I,
        recursive: bool,
        integrity: bool,
    ) -> Self
    where
        I: Iterator<Item = DatasetRef>,
    {
        Self {
            dataset_repo,
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
            .dataset_repo
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
                check_logical_hashes: true,
                replay_transformations: false,
            }
        } else {
            VerificationOptions {
                check_integrity: true,
                check_logical_hashes: true,
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

        for (_, res) in &verification_results {
            match res {
                Ok(_) => valid += 1,
                Err(_) => errors += 1,
            }
        }

        if valid != 0 {
            eprintln!(
                "{}",
                console::style(format!("{valid} dataset(s) are valid"))
                    .green()
                    .bold()
            );
        }
        if errors != 0 {
            Err(BatchError::new(
                format!("Failed to verify {errors} dataset(s)"),
                verification_results.into_iter().filter_map(|(id, res)| {
                    res.err().map(|e| (e, format!("Failed to verify {id}")))
                }),
            )
            .into())
        } else {
            Ok(())
        }
    }
}
