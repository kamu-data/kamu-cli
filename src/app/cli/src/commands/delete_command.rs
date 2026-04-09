// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_datasets::{DatasetRegistry, DeleteDatasetUseCase, DependencyGraphService};
use kamu_resources::ResourceKindDescriptor;

use super::{
    CLIError,
    Command,
    DeleteDatasetsCommand,
    DeleteResourcesCommand,
    DeleteResourcesScope,
};
use crate::cli_commands::validate_many_dataset_patterns_with_workspace;
use crate::output::OutputConfig;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    ResourceFacadeFactory,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
    ResourceSelectorResolutionService,
};
use crate::{ConfirmDeleteService, Interact, WorkspaceService, cli_value_parser as parsers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASETS_TARGET: &str = "datasets";
const ALL_TARGET: &str = "all";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct DeleteCommand {
    workspace_service: Arc<WorkspaceService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_kind_lookup_service: Arc<dyn ResourceKindLookupService>,
    resource_selector_resolution_service: Arc<dyn ResourceSelectorResolutionService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    interact: Arc<Interact>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    target: Option<String>,

    #[dill::component(explicit)]
    args: Vec<String>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    all: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    force: bool,

    #[dill::component(explicit)]
    ignore_not_found: bool,

    #[dill::component(explicit)]
    dry_run: bool,

    #[dill::component(explicit)]
    continue_on_error: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteCommand {
    // Mirrors `list` dispatch:
    // - `kamu delete` / `kamu delete datasets ...` => datasets mode
    // - `kamu delete all` => resource all-kinds mode
    // - `kamu delete storages warehouse` => resource-by-kind mode
    // - `kamu delete foo.bar` => datasets mode when `foo.bar` is not a known
    //   resource kind
    async fn resolve_request(&self) -> Result<ResolvedDeleteRequest, CLIError> {
        match self.target.as_deref() {
            None => Ok(ResolvedDeleteRequest::Datasets {
                dataset_args: self.args.clone(),
            }),
            Some(target) if target.eq_ignore_ascii_case(DATASETS_TARGET) => {
                Ok(ResolvedDeleteRequest::Datasets {
                    dataset_args: self.args.clone(),
                })
            }
            Some(target) if target.eq_ignore_ascii_case(ALL_TARGET) => {
                Ok(ResolvedDeleteRequest::ResourcesAll)
            }
            Some(target) => match self.resolve_resource_kind_descriptor(target).await? {
                Some(kind_descriptor) => Ok(ResolvedDeleteRequest::ResourcesByKind {
                    kind_descriptor,
                    resource_args: self.args.clone(),
                }),
                None => {
                    let mut dataset_args = Vec::with_capacity(self.args.len() + 1);
                    dataset_args.push(target.to_owned());
                    dataset_args.extend(self.args.clone());

                    Ok(ResolvedDeleteRequest::Datasets { dataset_args })
                }
            },
        }
    }

    async fn resolve_resource_kind_descriptor(
        &self,
        target: &str,
    ) -> Result<Option<ResourceKindDescriptor>, CLIError> {
        match self
            .resource_kind_lookup_service
            .resolve_kind_descriptor(
                self.explicit_context_name.as_deref(),
                target,
                ResourceKindLookupErrorOptions::new("Unsupported delete target")
                    .with_additional_targets([DATASETS_TARGET, ALL_TARGET]),
            )
            .await
        {
            Ok(kind_descriptor) => Ok(Some(kind_descriptor)),
            Err(CLIError::UsageError(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn make_delete_datasets_command(
        &self,
        request: &ResolvedDeleteRequest,
    ) -> Result<DeleteDatasetsCommand, CLIError> {
        let ResolvedDeleteRequest::Datasets { dataset_args } = request else {
            unreachable!();
        };

        let parsed_dataset_ref_patterns = dataset_args
            .clone()
            .into_iter()
            .map(|raw_pattern| {
                parsers::dataset_ref_pattern(&raw_pattern).map_err(CLIError::usage_error)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let dataset_ref_patterns = validate_many_dataset_patterns_with_workspace(
            self.workspace_service.as_ref(),
            parsed_dataset_ref_patterns,
        )?;

        Ok(DeleteDatasetsCommand::new(
            self.dataset_registry.clone(),
            self.delete_dataset.clone(),
            self.dependency_graph_service.clone(),
            self.confirm_delete_service.clone(),
            dataset_ref_patterns,
            self.all,
            self.recursive,
            self.force,
        ))
    }

    fn resolve_delete_resources_command(
        &self,
        request: &ResolvedDeleteRequest,
    ) -> Result<DeleteResourcesCommand, CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let scope = match request {
            ResolvedDeleteRequest::ResourcesAll => DeleteResourcesScope::All,
            ResolvedDeleteRequest::ResourcesByKind {
                kind_descriptor, ..
            } => DeleteResourcesScope::ByKind(kind_descriptor.clone()),
            ResolvedDeleteRequest::Datasets { .. } => unreachable!(),
        };

        let selector = match &scope {
            DeleteResourcesScope::All => None,
            DeleteResourcesScope::ByKind(_) => {
                if self.all {
                    None
                } else {
                    request.resource_selector()
                }
            }
        };

        Ok(DeleteResourcesCommand::new(
            resource_facade,
            self.resource_selector_resolution_service.clone(),
            self.resource_context_reporter.clone(),
            self.interact.clone(),
            self.output_config.clone(),
            resolved_context,
            scope,
            selector,
            self.all,
            self.force,
            self.ignore_not_found,
            self.dry_run,
            self.continue_on_error,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for DeleteCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        let request = self.resolve_request().await?;

        match &request {
            ResolvedDeleteRequest::Datasets { .. } => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only when deleting resources",
                    ));
                }
                if self.ignore_not_found {
                    return Err(CLIError::usage_error(
                        "--ignore-not-found is supported only when deleting resources",
                    ));
                }
                if self.dry_run {
                    return Err(CLIError::usage_error(
                        "--dry-run is supported only when deleting resources",
                    ));
                }
                if self.continue_on_error {
                    return Err(CLIError::usage_error(
                        "--continue-on-error is supported only when deleting resources",
                    ));
                }

                self.make_delete_datasets_command(&request)?
                    .validate_args()
                    .await
            }

            ResolvedDeleteRequest::ResourcesAll => {
                if self.recursive {
                    return Err(CLIError::usage_error(
                        "--recursive is supported only when deleting datasets",
                    ));
                }
                if self.all {
                    return Err(CLIError::usage_error(
                        "Deleting all resources does not accept --all",
                    ));
                }
                if !self.args.is_empty() {
                    return Err(CLIError::usage_error(
                        "Deleting all resources does not accept explicit selectors",
                    ));
                }

                DeleteResourcesCommand::validate_scope_and_selector(
                    &DeleteResourcesScope::All,
                    None,
                    self.all,
                )
            }

            ResolvedDeleteRequest::ResourcesByKind {
                kind_descriptor,
                resource_args,
            } => {
                if self.recursive {
                    return Err(CLIError::usage_error(
                        "--recursive is supported only when deleting datasets",
                    ));
                }

                match (resource_args.as_slice(), self.all) {
                    ([], false) => {
                        return Err(CLIError::usage_error(
                            "Specify a resource selector or pass --all",
                        ));
                    }
                    ([], true) | ([_], false) => {}
                    ([_selector], true) => {
                        return Err(CLIError::usage_error(
                            "You can either specify a resource selector or pass --all",
                        ));
                    }
                    ([_first, _second, ..], _) => {
                        return Err(CLIError::usage_error(
                            "Deleting resources accepts only one explicit selector in this MVP",
                        ));
                    }
                }

                DeleteResourcesCommand::validate_scope_and_selector(
                    &DeleteResourcesScope::ByKind(kind_descriptor.clone()),
                    if self.all {
                        None
                    } else {
                        resource_args.first().map(String::as_str)
                    },
                    self.all,
                )
            }
        }
    }

    async fn run(&self) -> Result<(), CLIError> {
        let request = self.resolve_request().await?;

        match &request {
            ResolvedDeleteRequest::Datasets { .. } => {
                self.make_delete_datasets_command(&request)?.run().await
            }
            ResolvedDeleteRequest::ResourcesAll | ResolvedDeleteRequest::ResourcesByKind { .. } => {
                self.resolve_delete_resources_command(&request)?.run().await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
enum ResolvedDeleteRequest {
    Datasets {
        dataset_args: Vec<String>,
    },
    ResourcesAll,
    ResourcesByKind {
        kind_descriptor: ResourceKindDescriptor,
        resource_args: Vec<String>,
    },
}

impl ResolvedDeleteRequest {
    fn resource_selector(&self) -> Option<String> {
        match self {
            Self::ResourcesByKind { resource_args, .. } => resource_args.first().cloned(),
            Self::Datasets { .. } | Self::ResourcesAll => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
