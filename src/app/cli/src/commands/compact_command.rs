// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{
    CompactionOptions,
    CompactionService,
    DatasetRepository,
    VerificationMultiListener,
    VerificationOptions,
    VerificationService,
};
use opendatafabric::{DatasetHandle, DatasetRef};

use crate::{BatchError, CLIError, Command, CompactionMultiProgress, VerificationMultiProgress};

pub struct CompactCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    verification_svc: Arc<dyn VerificationService>,
    compaction_svc: Arc<dyn CompactionService>,
    dataset_ref: DatasetRef,
    max_slice_size: u64,
    max_slice_records: u64,
    is_hard: bool,
    is_verify: bool,
    keep_metadata_only: bool,
}

impl CompactCommand {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        compaction_svc: Arc<dyn CompactionService>,
        dataset_ref: DatasetRef,
        max_slice_size: u64,
        max_slice_records: u64,
        is_hard: bool,
        is_verify: bool,
        keep_metadata_only: bool,
    ) -> Self {
        Self {
            dataset_repo,
            verification_svc,
            compaction_svc,
            dataset_ref,
            max_slice_size,
            max_slice_records,
            is_hard,
            is_verify,
            keep_metadata_only,
        }
    }

    async fn verify_dataset(&self, dataset_handle: &DatasetHandle) -> Result<(), CLIError> {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());
        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let result = self
            .verification_svc
            .verify(
                &dataset_handle.as_local_ref(),
                (None, None),
                VerificationOptions::default(),
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
    async fn run(&mut self) -> Result<(), CLIError> {
        if !self.is_hard {
            return Err(CLIError::usage_error(
                "Soft compactions are not yet supported",
            ));
        }
        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await
            .map_err(CLIError::failure)?;

        if self.is_verify {
            if let Err(err) = self.verify_dataset(&dataset_handle).await {
                eprintln!(
                    "{}",
                    console::style("Cannot perform compaction, dataset is invalid".to_string())
                        .red()
                );
                return Err(err);
            }
        }

        let progress = CompactionMultiProgress::new();
        let listener = Arc::new(progress.clone());

        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let compaction_results = self
            .compaction_svc
            .compact_multi(
                vec![dataset_handle.as_local_ref()],
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
