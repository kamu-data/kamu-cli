// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::TryStreamExt;
use kamu::domain::*;
use kamu::utils::datasets_filtering::filter_datasets_by_pattern;
use opendatafabric::*;

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;
use crate::VerificationMultiProgress;

type GenericVerificationResult = Result<Vec<VerificationResult>, CLIError>;

///////////////////////////////////////////////////////////////////////////////

pub struct VerifyCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRefPattern>,
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
        I: Iterator<Item = DatasetRefPattern>,
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
        let dataset_ref_pattern = self.refs.first().unwrap();

        let requests: Vec<_> = filter_datasets_by_pattern(
            self.dataset_repo.as_ref(),
            vec![dataset_ref_pattern.clone()],
        )
        .map_ok(|dataset_handle| VerificationRequest {
            dataset_ref: dataset_handle.as_local_ref(),
            block_range: (None, None),
        })
        .try_collect()
        .await?;

        if requests.is_empty() {
            return Ok(vec![]);
        }

        let listener = listener.unwrap();

        Ok(self
            .verification_svc
            .clone()
            .verify_multi(requests, options, Some(listener))
            .await)
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

        if verification_results.is_empty() {
            eprintln!(
                "{}",
                console::style("There are no datasets matching the pattern").yellow()
            );
            return Ok(());
        }

        let total_results = verification_results.len();

        let errors: Vec<_> = verification_results
            .into_iter()
            .filter_map(|result| match result.outcome {
                Ok(_) => None,
                Err(e) => Some((
                    e,
                    match result.dataset_handle {
                        None => "Failed to initiate verification".to_string(),
                        Some(hdl) => format!("Failed to verify {hdl}"),
                    },
                )),
            })
            .collect();

        if errors.is_empty() {
            eprintln!(
                "{}",
                console::style(format!("{total_results} dataset(s) are valid"))
                    .green()
                    .bold()
            );
            Ok(())
        } else {
            Err(BatchError::new(
                format!("Failed to verify {} dataset(s)", errors.len()),
                errors,
            )
            .into())
        }
    }
}
