// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
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
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    output_config: Arc<OutputConfig>,
    refs: Vec<DatasetRefPattern>,
    recursive: bool,
    integrity: bool,
}

struct RemoteRefDependency {
    source_dataset: String,
    dependencies: Vec<String>,
}

impl VerifyCommand {
    pub fn new<I>(
        dataset_repo: Arc<dyn DatasetRepository>,
        verification_svc: Arc<dyn VerificationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
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
            dependency_graph_service,
            remote_alias_reg,
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

        let dataset_ids: Vec<_> = filter_datasets_by_pattern(
            self.dataset_repo.as_ref(),
            vec![dataset_ref_pattern.clone()],
        )
        .map_ok(|dataset_handle| dataset_handle.id)
        .try_collect()
        .await?;

        let requests: Vec<_> = if self.recursive {
            self.dependency_graph_service
                .get_recursive_upstream_dependencies(dataset_ids)
                .await
                .int_err()?
                .map(|dataset_id| VerificationRequest {
                    dataset_ref: DatasetRef::ID(dataset_id),
                    block_range: (None, None),
                })
                .collect()
                .await
        } else {
            dataset_ids
                .iter()
                .map(|dataset_id| VerificationRequest {
                    dataset_ref: DatasetRef::ID(dataset_id.clone()),
                    block_range: (None, None),
                })
                .collect()
        };

        let (filtered_requests, missed_remote_dependencies) =
            self.check_remote_datasets(requests).await;

        if !missed_remote_dependencies.is_empty() {
            let missed_dependiency_warnings: Vec<String> = missed_remote_dependencies
                .iter()
                .map(|remote_ref_dependency| {
                    format!(
                        "Dataset:\n {}\nDependencies:\n {}",
                        remote_ref_dependency.source_dataset,
                        remote_ref_dependency.dependencies.join("\n"),
                    )
                })
                .collect();

            eprintln!(
                "{}",
                console::style(format!(
                    "Unable verify derivative transformation for some dataset(s). Please download \
                     next dependencies: \n{}\nor add --integrity flag and try again",
                    missed_dependiency_warnings.join("\n"),
                ))
                .yellow()
            );
        }

        if filtered_requests.is_empty() {
            return Ok(vec![]);
        }

        let listener = listener.unwrap();

        Ok(self
            .verification_svc
            .clone()
            .verify_multi(filtered_requests, options, Some(listener))
            .await)
    }

    // Return
    async fn check_remote_datasets(
        &self,
        verifiation_requests: Vec<VerificationRequest>,
    ) -> (Vec<VerificationRequest>, Vec<RemoteRefDependency>) {
        let mut result = vec![];
        let mut missed_dependencies = vec![];

        for verification_request in &verifiation_requests {
            if let Ok(dataset_handle) = self
                .dataset_repo
                .resolve_dataset_ref(&verification_request.dataset_ref)
                .await
            {
                let is_remote = self
                    .remote_alias_reg
                    .get_remote_aliases(&dataset_handle.as_local_ref())
                    .await
                    .unwrap()
                    .get_by_kind(RemoteAliasKind::Pull)
                    .next()
                    .is_some();
                if !is_remote || self.integrity {
                    result.push(verification_request.clone());
                    continue;
                }

                let dataset = self
                    .dataset_repo
                    .get_dataset(&dataset_handle.as_local_ref())
                    .await
                    .unwrap();
                let summary = dataset
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .unwrap();

                let mut current_missed_dependencies = vec![];

                for dependecy in summary.dependencies {
                    if self
                        .dataset_repo
                        .resolve_dataset_ref(&DatasetRef::ID(dependecy.clone()))
                        .await
                        .is_err()
                    {
                        current_missed_dependencies.push(dependecy.to_string());
                    }
                }
                if !current_missed_dependencies.is_empty() {
                    missed_dependencies.push(RemoteRefDependency {
                        source_dataset: dataset_handle.alias.to_string(),
                        dependencies: current_missed_dependencies,
                    });
                } else {
                    result.push(verification_request.clone());
                }
            }
        }

        (result, missed_dependencies)
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
