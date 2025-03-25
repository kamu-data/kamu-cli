// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::TryStreamExt as _;
use kamu::domain::{
    CompactDatasetUseCase,
    CompactionOptions,
    DatasetRegistry,
    VerificationMultiListener,
    VerificationOptions,
    VerificationRequest,
    VerifyDatasetUseCase,
};

use crate::{
    BatchError,
    CLIError,
    Command,
    CompactionMultiProgress,
    Interact,
    VerificationMultiProgress,
};

pub struct CompactCommand {
    interact: Arc<Interact>,
    compact_dataset_use_case: Arc<dyn CompactDatasetUseCase>,
    verify_dataset_use_case: Arc<dyn VerifyDatasetUseCase>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
    max_slice_size: u64,
    max_slice_records: u64,
    is_hard: bool,
    is_verify: bool,
    keep_metadata_only: bool,
}

impl CompactCommand {
    pub fn new(
        interact: Arc<Interact>,
        compact_dataset_use_case: Arc<dyn CompactDatasetUseCase>,
        verify_dataset_use_case: Arc<dyn VerifyDatasetUseCase>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
        max_slice_size: u64,
        max_slice_records: u64,
        is_hard: bool,
        is_verify: bool,
        keep_metadata_only: bool,
    ) -> Self {
        Self {
            interact,
            compact_dataset_use_case,
            verify_dataset_use_case,
            dataset_registry,
            dataset_ref_patterns,
            max_slice_size,
            max_slice_records,
            is_hard,
            is_verify,
            keep_metadata_only,
        }
    }

    async fn verify_dataset(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), CLIError> {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());
        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let result = self
            .verify_dataset_use_case
            .execute(
                VerificationRequest {
                    target: dataset_handle.clone(),
                    block_range: (None, None),
                    options: VerificationOptions::default(),
                },
                listener.begin_verify(dataset_handle),
            )
            .await;

        listener.finish();
        draw_thread.join().unwrap();

        result.outcome.map_err(CLIError::failure)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompactCommand {
    async fn run(&self) -> Result<(), CLIError> {
        if self.dataset_ref_patterns.is_empty() {
            return Err(CLIError::usage_error("Specify a dataset or a pattern"));
        }

        if !self.is_hard {
            return Err(CLIError::usage_error(
                "Soft compactions are not yet supported",
            ));
        }

        let dataset_handles: Vec<odf::DatasetHandle> = {
            kamu::utils::datasets_filtering::filter_datasets_by_local_pattern(
                self.dataset_registry.as_ref(),
                self.dataset_ref_patterns.clone(),
            )
            .try_collect()
            .await?
        };

        self.interact.require_confirmation(format!(
            "{}\n  {}\n{}",
            console::style(
                "You are about to perform a hard compaction of the following dataset(s):"
            )
            .yellow(),
            itertools::join(dataset_handles.iter().map(|h| &h.alias), "\n  "),
            console::style("This operation is history-altering and irreversible!").yellow(),
        ))?;

        if self.is_verify {
            for hdl in &dataset_handles {
                if let Err(err) = self.verify_dataset(hdl).await {
                    eprintln!(
                        "{}",
                        console::style("Cannot perform compaction, dataset is invalid".to_string())
                            .red()
                    );
                    return Err(err);
                }
            }
        }

        let progress = CompactionMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let compaction_results = self
            .compact_dataset_use_case
            .execute_multi(
                dataset_handles,
                CompactionOptions {
                    max_slice_size: Some(self.max_slice_size),
                    max_slice_records: Some(self.max_slice_records),
                    keep_metadata_only: self.keep_metadata_only,
                },
                Some(listener.clone()),
            )
            .await;

        let total_results = compaction_results.len();

        listener.finish();
        draw_thread.join().unwrap();

        let errors: Vec<_> = compaction_results
            .into_iter()
            .filter_map(|response| match response.result {
                Ok(_) => None,
                Err(e) => Some((e, format!("Failed to compact {}", response.dataset_ref))),
            })
            .collect();

        if errors.is_empty() {
            eprintln!(
                "{}",
                console::style(format!("{total_results} dataset(s) were compacted"))
                    .green()
                    .bold()
            );
            Ok(())
        } else {
            Err(BatchError::new(
                format!(
                    "Failed to compact {}/{total_results} dataset(s)",
                    errors.len()
                ),
                errors,
            )
            .into())
        }
    }
}
