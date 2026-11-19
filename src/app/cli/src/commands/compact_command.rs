// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use futures::TryStreamExt as _;
use kamu::domain::{
    CompactionJob,
    CompactionMultiListener,
    CompactionOptions,
    VerificationOptions,
    VerifyJob,
};
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;
use kamu_datasets::DatasetRegistry;

use super::common::verify_dataset;
use crate::{
    BatchError,
    CLIError,
    Command,
    CompactionMultiProgress,
    Interact,
    VerificationMultiProgress,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct CompactCommand {
    interact: Arc<Interact>,
    catalog: dill::Catalog,
    compaction_job: Arc<dyn CompactionJob>,
    verify_job: Arc<dyn VerifyJob>,

    #[dill::component(explicit)]
    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,

    #[dill::component(explicit)]
    max_slice_size: u64,

    #[dill::component(explicit)]
    max_slice_records: u64,

    #[dill::component(explicit)]
    is_hard: bool,

    #[dill::component(explicit)]
    is_verify: bool,

    #[dill::component(explicit)]
    keep_metadata_only: bool,
}

impl CompactCommand {
    async fn verify_dataset(&self, dataset_handle: &odf::DatasetHandle) -> Result<(), CLIError> {
        let progress = VerificationMultiProgress::new();
        let listener = Arc::new(progress.clone());
        let draw_thread = std::thread::spawn(move || {
            progress.draw();
        });

        let result = verify_dataset(
            self.verify_job.as_ref(),
            dataset_handle.clone(),
            &VerificationOptions::default(),
            Some(listener.as_ref()),
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

        let dataset_handles = transaction!(|dataset_registry: Arc<dyn DatasetRegistry>| {
            filter_datasets_by_local_pattern(
                dataset_registry.as_ref(),
                self.dataset_ref_patterns.clone(),
            )
            .try_collect::<Vec<_>>()
            .await?
        });

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

        let options = CompactionOptions {
            max_slice_size: Some(self.max_slice_size),
            max_slice_records: Some(self.max_slice_records),
            keep_metadata_only: self.keep_metadata_only,
        };

        // Each dataset is committed as soon as it is compacted
        let mut errors = Vec::new();
        for hdl in &dataset_handles {
            if let Err(e) = self
                .compaction_job
                .run(hdl, options.clone(), listener.begin_compact(hdl))
                .await
            {
                errors.push((e, format!("Failed to compact {}", hdl.as_local_ref())));
            }
        }
        let total_results = dataset_handles.len();

        listener.finish();
        draw_thread.join().unwrap();

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
