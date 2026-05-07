// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::TenancyConfig;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{DatasetRegistry, DeleteDatasetUseCase, DependencyGraphService};

use super::{CLIError, Command, DeleteDatasetsCommand, DeleteResourcesCommand};
use crate::cli_commands::validate_many_dataset_patterns_with_workspace;
use crate::output::OutputConfig;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    ResourceFacadeFactory,
    ResourceKindLookupService,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceSelectionSyntaxService,
};
use crate::{ConfirmDeleteService, Interact, WorkspaceService, cli_value_parser as parsers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASETS_TARGET: &str = "datasets";
const ALL_TARGET: &str = "all";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct DeleteCommand {
    tenancy_config: TenancyConfig,
    workspace_service: Arc<WorkspaceService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_kind_lookup_service: Arc<dyn ResourceKindLookupService>,
    resource_selection_syntax_service: Arc<dyn ResourceSelectionSyntaxService>,
    resource_selection_resolution_service: Arc<dyn ResourceSelectionResolutionService>,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteCommand {
    async fn resolved_request(&self) -> Result<ResolvedDeleteRequest, CLIError> {
        DeleteRequestResolver::new(
            self.resource_kind_lookup_service.as_ref(),
            self.resource_selection_syntax_service.as_ref(),
            DeleteRequestResolverParams {
                target: self.target.as_deref(),
                args: &self.args,
                explicit_context_name: self.explicit_context_name.as_deref(),
                all: self.all,
            },
        )
        .resolve()
        .await
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
            self.tenancy_config,
            self.dataset_registry.clone(),
            self.delete_dataset.clone(),
            self.dependency_graph_service.clone(),
            self.confirm_delete_service.clone(),
            self.current_account_subject.clone(),
            self.rebac_service.clone(),
            dataset_ref_patterns,
            self.all,
            self.recursive,
            self.force,
            self.ignore_not_found,
            self.dry_run,
        ))
    }

    fn resolve_delete_resources_command(
        &self,
        request: &ResolvedDeleteRequest,
    ) -> Result<DeleteResourcesCommand, CLIError> {
        let ResolvedDeleteRequest::Resources { syntax } = request else {
            unreachable!();
        };

        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        Ok(DeleteResourcesCommand::new(
            resource_facade,
            self.resource_selection_resolution_service.clone(),
            self.resource_context_reporter.clone(),
            self.interact.clone(),
            self.output_config.clone(),
            resolved_context,
            syntax.clone(),
            self.force,
            self.ignore_not_found,
            self.dry_run,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for DeleteCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        let request = self.resolved_request().await?;

        match &request {
            ResolvedDeleteRequest::Datasets { .. } => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only when deleting resources",
                    ));
                }

                self.make_delete_datasets_command(&request)?
                    .validate_args()
                    .await
            }

            ResolvedDeleteRequest::Resources { .. } => {
                if self.recursive {
                    return Err(CLIError::usage_error(
                        "--recursive is supported only when deleting datasets",
                    ));
                }
                self.resolve_delete_resources_command(&request)?
                    .validate_args()
                    .await
            }

            ResolvedDeleteRequest::Mixed => Err(CLIError::usage_error(
                "Mixed dataset/resource deletion is not implemented",
            )),
        }
    }

    async fn run(&self) -> Result<(), CLIError> {
        let request = self.resolved_request().await?;

        match &request {
            ResolvedDeleteRequest::Datasets { .. } => {
                self.make_delete_datasets_command(&request)?.run().await
            }
            ResolvedDeleteRequest::Resources { .. } => {
                self.resolve_delete_resources_command(&request)?.run().await
            }
            ResolvedDeleteRequest::Mixed => Err(CLIError::usage_error(
                "Mixed dataset/resource deletion is not implemented",
            )),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
enum ResolvedDeleteRequest {
    Datasets { dataset_args: Vec<String> },
    Resources { syntax: ResourceSelectionSyntax },
    Mixed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteRequestResolverParams<'a> {
    target: Option<&'a str>,
    args: &'a [String],
    explicit_context_name: Option<&'a str>,
    all: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteRequestResolver<'a> {
    resource_kind_lookup_service: &'a dyn ResourceKindLookupService,
    resource_selection_syntax_service: &'a dyn ResourceSelectionSyntaxService,
    params: DeleteRequestResolverParams<'a>,
}

impl<'a> DeleteRequestResolver<'a> {
    fn new(
        resource_kind_lookup_service: &'a dyn ResourceKindLookupService,
        resource_selection_syntax_service: &'a dyn ResourceSelectionSyntaxService,
        params: DeleteRequestResolverParams<'a>,
    ) -> Self {
        Self {
            resource_kind_lookup_service,
            resource_selection_syntax_service,
            params,
        }
    }

    // Mirrors `list` dispatch, but with delete-specific dataset/resource
    // precedence:
    // - `kamu delete` / `kamu delete datasets ...` => datasets mode
    // - `kamu delete all` => resource all-kinds mode
    // - `kamu delete storages warehouse` => resource same-kind mode
    // - `kamu delete foo.bar` => datasets mode when `foo.bar` is not a known
    //   resource kind
    // - `kamu delete vs/foo` => resource slash mode when `vs` is a known resource
    //   prefix
    async fn resolve(&self) -> Result<ResolvedDeleteRequest, CLIError> {
        match self.params.target {
            None => {
                return Ok(ResolvedDeleteRequest::Datasets {
                    dataset_args: self.params.args.to_vec(),
                });
            }
            Some(target) if target.eq_ignore_ascii_case(DATASETS_TARGET) => {
                return Ok(ResolvedDeleteRequest::Datasets {
                    dataset_args: self.params.args.to_vec(),
                });
            }
            _ => {}
        }

        let raw_args = self.raw_args();

        if raw_args
            .first()
            .is_some_and(|arg| arg.eq_ignore_ascii_case(ALL_TARGET))
        {
            return self.resolve_resource_request(raw_args).await;
        }

        let contains_slash = raw_args.iter().any(|arg| arg.contains('/'));
        let contains_plain = raw_args.iter().any(|arg| !arg.contains('/'));

        let first_arg_is_resource_prefix = self
            .is_supported_resource_prefix(raw_args.first().expect("target is present"))
            .await?;

        if !contains_slash {
            if first_arg_is_resource_prefix {
                if self.params.all {
                    return self
                        .resolve_resource_request(Self::with_resource_all(raw_args))
                        .await;
                }
                return self.resolve_resource_request(raw_args).await;
            }

            return Ok(ResolvedDeleteRequest::Datasets {
                dataset_args: raw_args,
            });
        }

        if contains_plain {
            if first_arg_is_resource_prefix {
                if self.params.all {
                    return Err(CLIError::usage_error(
                        "You can either specify a resource selector or pass --all",
                    ));
                }
                return self.resolve_resource_request(raw_args).await;
            }

            return Err(CLIError::usage_error(
                "Cannot mix plain and slash delete selectors",
            ));
        }

        match self.classify_slash_request(raw_args).await? {
            ClassifiedSlashDeleteRequest::Datasets(dataset_args) => {
                Ok(ResolvedDeleteRequest::Datasets { dataset_args })
            }
            ClassifiedSlashDeleteRequest::Resources(raw_args) => {
                self.resolve_resource_request(raw_args).await
            }
            ClassifiedSlashDeleteRequest::Mixed => Ok(ResolvedDeleteRequest::Mixed),
        }
    }

    fn with_resource_all(mut raw_args: Vec<String>) -> Vec<String> {
        raw_args.push(ALL_TARGET.to_owned());
        raw_args
    }

    fn raw_args(&self) -> Vec<String> {
        let mut raw_args =
            Vec::with_capacity(self.params.args.len() + usize::from(self.params.target.is_some()));
        if let Some(target) = self.params.target {
            raw_args.push(target.to_owned());
        }
        raw_args.extend(self.params.args.iter().cloned());
        raw_args
    }

    async fn is_supported_resource_prefix(&self, prefix: &str) -> Result<bool, CLIError> {
        Ok(self
            .resource_kind_lookup_service
            .list_supported_kinds(self.params.explicit_context_name)
            .await?
            .iter()
            .any(|descriptor| descriptor.matches_selector(prefix)))
    }

    async fn classify_slash_request(
        &self,
        raw_args: Vec<String>,
    ) -> Result<ClassifiedSlashDeleteRequest, CLIError> {
        let supported_kinds = self
            .resource_kind_lookup_service
            .list_supported_kinds(self.params.explicit_context_name)
            .await?;

        Ok(Self::classify_slash_request_with(raw_args, |prefix| {
            supported_kinds
                .iter()
                .any(|descriptor| descriptor.matches_selector(prefix))
        }))
    }

    fn classify_slash_request_with<F>(
        raw_args: Vec<String>,
        is_supported_resource_prefix: F,
    ) -> ClassifiedSlashDeleteRequest
    where
        F: Fn(&str) -> bool,
    {
        let mut dataset_args = Vec::new();
        let mut resource_args = Vec::new();

        for arg in raw_args {
            let Some((prefix, suffix)) = arg.split_once('/') else {
                unreachable!("slash-only classifier received a plain selector");
            };

            // `dataset/...` and `datasets/...` are an explicit escape hatch that forces
            // legacy dataset interpretation even if the prefix collides with a resource
            // kind.
            if prefix.eq_ignore_ascii_case("dataset")
                || prefix.eq_ignore_ascii_case(DATASETS_TARGET)
            {
                dataset_args.push(suffix.to_owned());
            } else if is_supported_resource_prefix(prefix) {
                resource_args.push(arg);
            } else {
                dataset_args.push(arg);
            }
        }

        match (!dataset_args.is_empty(), !resource_args.is_empty()) {
            (true, false) => ClassifiedSlashDeleteRequest::Datasets(dataset_args),
            (false, true) => ClassifiedSlashDeleteRequest::Resources(resource_args),
            (true, true) => ClassifiedSlashDeleteRequest::Mixed,
            (false, false) => unreachable!("slash request must contain at least one selector"),
        }
    }

    async fn resolve_resource_request(
        &self,
        raw_args: Vec<String>,
    ) -> Result<ResolvedDeleteRequest, CLIError> {
        // Delete reuses the `get` selector grammar, but broad selectors shadowing
        // narrower ones are rejected for destructive commands instead of
        // downgraded to warnings.
        let syntax = self
            .resource_selection_syntax_service
            .parse_get_args(self.params.explicit_context_name, &raw_args)
            .await?;

        if !syntax.shadowed_selectors.is_empty() {
            let shadowed_selectors = syntax
                .shadowed_selectors
                .iter()
                .map(|selector| format!("`{}`", selector.selector_input))
                .collect::<Vec<_>>()
                .join(", ");

            return Err(CLIError::usage_error(format!(
                "Delete selectors must not be shadowed by a broader selector: {shadowed_selectors}"
            )));
        }

        Ok(ResolvedDeleteRequest::Resources { syntax })
    }
}

enum ClassifiedSlashDeleteRequest {
    Datasets(Vec<String>),
    Resources(Vec<String>),
    Mixed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::{ClassifiedSlashDeleteRequest, DeleteRequestResolver};

    #[test]
    fn test_classify_slash_request_routes_resource_prefixes_to_resources() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["vs/foo".to_owned(), "ss/bar".to_owned()],
            |prefix| matches!(prefix, "vs" | "ss"),
        );

        assert!(matches!(
            request,
            ClassifiedSlashDeleteRequest::Resources(args)
                if args == vec!["vs/foo".to_owned(), "ss/bar".to_owned()]
        ));
    }

    #[test]
    fn test_classify_slash_request_preserves_unknown_prefixes_as_datasets() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["account/foo".to_owned()],
            |_| false,
        );

        assert!(matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["account/foo".to_owned()]
        ));
    }

    #[test]
    fn test_classify_slash_request_strips_dataset_pseudo_kind_prefix() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["datasets/foo".to_owned(), "dataset/bar".to_owned()],
            |_| false,
        );

        assert!(matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["foo".to_owned(), "bar".to_owned()]
        ));
    }

    #[test]
    fn test_classify_slash_request_marks_mixed_requests() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["dataset/foo".to_owned(), "vs/bar".to_owned()],
            |prefix| prefix == "vs",
        );

        assert!(matches!(request, ClassifiedSlashDeleteRequest::Mixed));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
