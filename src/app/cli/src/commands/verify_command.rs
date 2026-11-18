// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use database_common::transaction;
use futures::{StreamExt, TryStreamExt};
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::*;
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;
use kamu_datasets::{DatasetRegistry, DependencyGraphService};

use super::{BatchError, CLIError, Command};
use crate::VerificationMultiProgress;
use crate::output::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type GenericVerificationResult = Result<Vec<VerificationResult>, CLIError>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct VerifyCommand {
    catalog: dill::Catalog,
    verify_job: Arc<dyn VerifyJob>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    refs: Vec<odf::DatasetRefPattern>,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    integrity: bool,
}

struct RemoteRefDependency {
    source_dataset: String,
    dependencies: Vec<String>,
}

/// The datasets to verify, and those that cannot be
struct VerificationSelection {
    dataset_handles: Vec<odf::DatasetHandle>,
    unresolved_results: Vec<VerificationResult>,
    missed_remote_dependencies: Vec<RemoteRefDependency>,
}

impl VerifyCommand {
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
        multi_listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> GenericVerificationResult {
        let selection = transaction!(
            |dataset_registry: Arc<dyn DatasetRegistry>,
             graph: Arc<dyn DependencyGraphService>,
             pull_sources: Arc<dyn DatasetPullSourceResolver>| {
                self.select_datasets(
                    dataset_registry.as_ref(),
                    graph.as_ref(),
                    pull_sources.as_ref(),
                )
                .await?
            }
        );

        let VerificationSelection {
            dataset_handles,
            mut unresolved_results,
            missed_remote_dependencies,
        } = selection;

        if !missed_remote_dependencies.is_empty() {
            let missed_dependency_warnings: Vec<String> = missed_remote_dependencies
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
                    missed_dependency_warnings.join("\n"),
                ))
                .yellow()
            );
        }

        if dataset_handles.is_empty() {
            return Ok(vec![]);
        }

        // Each verification opens its own short transaction and holds none
        // while it reads data or replays transforms
        let mut verification_results = Vec::new();
        verification_results.append(&mut unresolved_results);
        for hdl in dataset_handles {
            let listener = multi_listener
                .as_ref()
                .and_then(|multi_listener| multi_listener.begin_verify(&hdl));
            let request = VerificationRequest {
                target: hdl,
                block_range: (None, None),
                options: options.clone(),
            };
            verification_results.push(self.verify_job.run(request, listener).await);
        }

        Ok(verification_results)
    }

    /// The datasets matching the pattern, with their upstream dependencies
    /// when recursive, except remote datasets whose inputs are missing
    async fn select_datasets(
        &self,
        dataset_registry: &dyn DatasetRegistry,
        dependency_graph_service: &dyn DependencyGraphService,
        pull_source_resolver: &dyn DatasetPullSourceResolver,
    ) -> Result<VerificationSelection, CLIError> {
        let dataset_ref_pattern = self.refs.first().unwrap();

        let dataset_handles: Vec<_> =
            filter_datasets_by_local_pattern(dataset_registry, vec![dataset_ref_pattern.clone()])
                .try_collect()
                .await?;

        let mut unresolved_results = Vec::new();

        let dataset_handles: Vec<_> = if self.recursive {
            let input_dataset_ids = dataset_handles.into_iter().map(|hdl| hdl.id).collect();

            let all_dataset_ids = dependency_graph_service
                .get_recursive_upstream_dependencies(input_dataset_ids)
                .await
                .int_err()?
                .map(Cow::Owned)
                .collect::<Vec<Cow<'static, _>>>()
                .await;
            let resolution_results = dataset_registry
                .resolve_multiple_dataset_handles_by_ids(&all_dataset_ids)
                .await
                .int_err()?;

            for (_, e) in resolution_results.unresolved_datasets {
                unresolved_results.push(VerificationResult::err_no_handle(e));
            }

            resolution_results.resolved_handles
        } else {
            dataset_handles
        };

        let (dataset_handles, missed_remote_dependencies) = self
            .detect_remote_datasets(
                dataset_registry,
                dependency_graph_service,
                pull_source_resolver,
                dataset_handles,
            )
            .await?;

        Ok(VerificationSelection {
            dataset_handles,
            unresolved_results,
            missed_remote_dependencies,
        })
    }

    // Return tuple with filtered VerificationRequests(check existing)
    //   with a list of missed remote dependencies
    async fn detect_remote_datasets(
        &self,
        dataset_registry: &dyn DatasetRegistry,
        dependency_graph_service: &dyn DependencyGraphService,
        pull_source_resolver: &dyn DatasetPullSourceResolver,
        dataset_handles: Vec<odf::DatasetHandle>,
    ) -> Result<(Vec<odf::DatasetHandle>, Vec<RemoteRefDependency>), InternalError> {
        let mut result = vec![];
        let mut missed_dependencies = vec![];

        for hdl in dataset_handles {
            let is_remote = pull_source_resolver.has_pull_source(&hdl).await?;
            if !is_remote || self.integrity {
                result.push(hdl);
                continue;
            }

            let upstream_dependencies: Vec<odf::DatasetID> = dependency_graph_service
                .get_upstream_dependencies(&hdl.id)
                .await
                .collect()
                .await;

            let mut current_missed_dependencies = vec![];

            for dependency in upstream_dependencies {
                if dataset_registry
                    .resolve_dataset_handle_by_ref(&dependency.as_local_ref())
                    .await
                    .is_err()
                {
                    current_missed_dependencies.push(dependency.to_string());
                }
            }
            if !current_missed_dependencies.is_empty() {
                missed_dependencies.push(RemoteRefDependency {
                    source_dataset: hdl.alias.to_string(),
                    dependencies: current_missed_dependencies,
                });
            } else {
                result.push(hdl);
            }
        }

        Ok((result, missed_dependencies))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for VerifyCommand {
    async fn run(&self) -> Result<(), CLIError> {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
