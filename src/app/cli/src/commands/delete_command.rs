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
use kamu_datasets::{DatasetRegistry, DeleteDatasetUseCase};
use kamu_resources::ResourceTypeDescriptor;

use super::{CLIError, Command, DeleteDatasetsCommand, DeleteResourcesCommand};
use crate::cli_commands::validate_many_dataset_patterns_with_workspace;
use crate::output::OutputConfig;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    ANY_SELECTOR,
    BareTypePolicy,
    ResourceFacadeFactory,
    ResourceLabelSelectorParser,
    ResourceSelectionResolutionService,
    ResourceSelectionScanner,
    ResourceSelectionSyntax,
    ResourceSelectionSyntaxService,
    ResourceTypeLookupService,
    is_dataset_target,
    is_resource_id,
};
use crate::{ConfirmDeleteService, Interact, WorkspaceService, cli_value_parser as parsers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Strips the `dataset/` or `datasets/` pseudo-type prefix, yielding the
/// dataset alias it guards.
///
/// The remainder is deliberately not validated here: it is a dataset reference,
/// not a resource selector, and the dataset layer owns its own grammar.
fn strip_dataset_pseudo_type_prefix(arg: &str) -> Option<&str> {
    let (prefix, alias) = arg.split_once('/')?;

    is_dataset_target(prefix).then_some(alias)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct DeleteCommand {
    tenancy_config: TenancyConfig,
    current_account_subject: Arc<CurrentAccountSubject>,
    workspace_service: Arc<WorkspaceService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_type_lookup_service: Arc<dyn ResourceTypeLookupService>,
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

    #[dill::component(explicit)]
    label_selectors: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteCommand {
    async fn resolved_request(&self) -> Result<ResolvedDeleteRequest, CLIError> {
        DeleteRequestResolver::new(
            self.resource_type_lookup_service.as_ref(),
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
        let dataset_args = match request {
            ResolvedDeleteRequest::Datasets { dataset_args }
            | ResolvedDeleteRequest::Mixed { dataset_args, .. } => dataset_args,
            ResolvedDeleteRequest::Resources { .. } => {
                unreachable!();
            }
        };

        self.make_delete_datasets_command_from_args(dataset_args)
    }

    fn make_delete_datasets_command_from_args(
        &self,
        dataset_args: &[String],
    ) -> Result<DeleteDatasetsCommand, CLIError> {
        let parsed_dataset_ref_patterns = dataset_args
            .iter()
            .map(|raw_pattern| {
                parsers::dataset_ref_pattern(raw_pattern).map_err(CLIError::usage_error)
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
            self.confirm_delete_service.clone(),
            self.current_account_subject.clone(),
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
        let syntax = match request {
            ResolvedDeleteRequest::Resources { syntax }
            | ResolvedDeleteRequest::Mixed { syntax, .. } => syntax,
            ResolvedDeleteRequest::Datasets { .. } => {
                unreachable!();
            }
        };

        self.resolve_delete_resources_command_from_syntax(syntax.clone())
    }

    fn label_selector_unsupported_for_datasets_error() -> CLIError {
        CLIError::usage_error("Label selectors are not supported when deleting datasets")
    }

    fn resolve_delete_resources_command_from_syntax(
        &self,
        syntax: ResourceSelectionSyntax,
    ) -> Result<DeleteResourcesCommand, CLIError> {
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
            syntax,
            ResourceLabelSelectorParser::parse(&self.label_selectors)?,
            self.force,
            self.ignore_not_found,
            self.dry_run,
        ))
    }

    async fn run_mixed(
        &self,
        dataset_args: &[String],
        syntax: ResourceSelectionSyntax,
    ) -> Result<(), CLIError> {
        let delete_datasets_command = self.make_delete_datasets_command_from_args(dataset_args)?;
        let delete_resources_command = self.resolve_delete_resources_command_from_syntax(syntax)?;

        let prepared_datasets = delete_datasets_command.validate_and_prepare().await?;
        let prepared_resources = delete_resources_command.validate_and_prepare().await?;

        delete_datasets_command
            .run_prepared(prepared_datasets)
            .await?;
        delete_resources_command
            .run_prepared(prepared_resources)
            .await
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

                if !self.label_selectors.is_empty() {
                    return Err(Self::label_selector_unsupported_for_datasets_error());
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

            ResolvedDeleteRequest::Mixed { .. } => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only for pure resource deletion",
                    ));
                }

                // Applying the filter to only the resource half would still
                // delete every named dataset unfiltered.
                if !self.label_selectors.is_empty() {
                    return Err(Self::label_selector_unsupported_for_datasets_error());
                }

                self.make_delete_datasets_command(&request)?
                    .validate_args()
                    .await?;
                self.resolve_delete_resources_command(&request)?
                    .validate_args()
                    .await
            }
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
            ResolvedDeleteRequest::Mixed {
                dataset_args,
                syntax,
            } => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only for pure resource deletion",
                    ));
                }

                self.run_mixed(dataset_args, syntax.clone()).await
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
    Resources {
        syntax: ResourceSelectionSyntax,
    },
    Mixed {
        dataset_args: Vec<String>,
        syntax: ResourceSelectionSyntax,
    },
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
    resource_type_lookup_service: &'a dyn ResourceTypeLookupService,
    resource_selection_syntax_service: &'a dyn ResourceSelectionSyntaxService,
    params: DeleteRequestResolverParams<'a>,
}

impl<'a> DeleteRequestResolver<'a> {
    fn new(
        resource_type_lookup_service: &'a dyn ResourceTypeLookupService,
        resource_selection_syntax_service: &'a dyn ResourceSelectionSyntaxService,
        params: DeleteRequestResolverParams<'a>,
    ) -> Self {
        Self {
            resource_type_lookup_service,
            resource_selection_syntax_service,
            params,
        }
    }

    // Mirrors `list` dispatch, but with delete-specific dataset/resource
    // precedence:
    // - `kamu delete` / `kamu delete datasets ...` => datasets mode
    // - `kamu delete %` => resource all-types mode
    // - `kamu delete storages warehouse` => resource same-type mode
    // - `kamu delete foo.bar` => datasets mode when `foo.bar` is not a known
    //   resource type
    // - `kamu delete vs/foo` => resource slash mode when `vs` is a known resource
    //   prefix
    async fn resolve(&self) -> Result<ResolvedDeleteRequest, CLIError> {
        match self.params.target {
            None => {
                return Ok(ResolvedDeleteRequest::Datasets {
                    dataset_args: self.params.args.to_vec(),
                });
            }
            Some(target) if is_dataset_target(target) => {
                return Ok(ResolvedDeleteRequest::Datasets {
                    dataset_args: self.params.args.to_vec(),
                });
            }
            _ => {}
        }

        let raw_args = self.raw_args();

        // No descriptor matches `%`, so without this the all-types form would
        // fall through to the dataset path and glob-delete every dataset.
        if raw_args.first().is_some_and(|arg| arg == ANY_SELECTOR) {
            let raw_args = if self.params.all {
                Self::with_resource_all(raw_args)
            } else {
                raw_args
            };
            return self.resolve_resource_request(raw_args).await;
        }

        // Routing guard, not grammar: this decides dataset-vs-resource before
        // the selector grammar is involved. The resource path's own mixing
        // error is raised later by `ResourceSelectionSyntaxParser`.
        let contains_slash = raw_args.iter().any(|arg| arg.contains('/'));
        let contains_plain = raw_args.iter().any(|arg| !arg.contains('/'));

        let first_arg_is_resource_prefix = self
            .matches_resource_target_prefix(raw_args.first().expect("target is present"))
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

            // A bare ID has no type prefix but is still unambiguous (IDs are
            // globally unique), so route it as a resource, not a dataset.
            if raw_args.len() == 1 && is_resource_id(&raw_args[0]) {
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
            ClassifiedSlashDeleteRequest::Mixed {
                dataset_args,
                resource_args,
            } => {
                let syntax = self.resolve_resource_syntax(resource_args).await?;
                Ok(ResolvedDeleteRequest::Mixed {
                    dataset_args,
                    syntax,
                })
            }
        }
    }

    fn with_resource_all(mut raw_args: Vec<String>) -> Vec<String> {
        raw_args.push(ANY_SELECTOR.to_owned());
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

    async fn matches_resource_target_prefix(&self, prefix: &str) -> Result<bool, CLIError> {
        let supported_resource_types = self
            .resource_type_lookup_service
            .list_supported_resource_types(self.params.explicit_context_name)
            .await?;

        Ok(Self::matches_resource_target_prefix_with(
            &supported_resource_types,
            prefix,
        ))
    }

    async fn classify_slash_request(
        &self,
        raw_args: Vec<String>,
    ) -> Result<ClassifiedSlashDeleteRequest, CLIError> {
        let supported_resource_types = self
            .resource_type_lookup_service
            .list_supported_resource_types(self.params.explicit_context_name)
            .await?;

        Ok(Self::classify_slash_request_with(raw_args, |prefix| {
            Self::matches_resource_slash_prefix_with(&supported_resource_types, prefix)
        }))
    }

    fn matches_resource_target_prefix_with(
        supported_resource_types: &[ResourceTypeDescriptor],
        prefix: &str,
    ) -> bool {
        supported_resource_types
            .iter()
            .any(|descriptor| descriptor.matches_selector(prefix))
    }

    /// A `%`-carrying type half claims the resource path even when it names no
    /// supported type, so the user gets the resource-selector usage error
    /// rather than a dataset error. Dataset names may contain `%` but never
    /// appear as the type half.
    fn matches_resource_slash_prefix_with(
        supported_resource_types: &[ResourceTypeDescriptor],
        prefix: &str,
    ) -> bool {
        prefix.contains('%')
            || Self::matches_resource_target_prefix_with(supported_resource_types, prefix)
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
            // `dataset/...` and `datasets/...` are an explicit escape hatch that forces
            // legacy dataset interpretation even if the prefix collides with a resource
            // type. Stripped before scanning: the remainder is a dataset alias, which
            // may itself be account-qualified (`dataset/alice/foo`) and so carry more
            // separators than the selector grammar permits.
            if let Some(alias) = strip_dataset_pseudo_type_prefix(&arg) {
                dataset_args.push(alias.to_owned());
                continue;
            }

            let Ok(selector) =
                ResourceSelectionScanner::scan_selector_arg(&arg, BareTypePolicy::Allow)
            else {
                // Malformed args stay on the resource path so the selector
                // grammar reports them with a caret, rather than being silently
                // reinterpreted as a dataset name here.
                resource_args.push(arg);
                continue;
            };

            let (prefix, Some(_)) = (selector.type_half, selector.name_half) else {
                unreachable!("slash-only classifier received a plain selector");
            };

            if is_supported_resource_prefix(prefix) {
                resource_args.push(arg);
            } else {
                dataset_args.push(arg);
            }
        }

        match (!dataset_args.is_empty(), !resource_args.is_empty()) {
            (true, false) => ClassifiedSlashDeleteRequest::Datasets(dataset_args),
            (false, true) => ClassifiedSlashDeleteRequest::Resources(resource_args),
            (true, true) => ClassifiedSlashDeleteRequest::Mixed {
                dataset_args,
                resource_args,
            },
            (false, false) => unreachable!("slash request must contain at least one selector"),
        }
    }

    async fn resolve_resource_request(
        &self,
        raw_args: Vec<String>,
    ) -> Result<ResolvedDeleteRequest, CLIError> {
        let syntax = self.resolve_resource_syntax(raw_args).await?;
        Ok(ResolvedDeleteRequest::Resources { syntax })
    }

    async fn resolve_resource_syntax(
        &self,
        raw_args: Vec<String>,
    ) -> Result<ResourceSelectionSyntax, CLIError> {
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

        Ok(syntax)
    }
}

#[derive(Debug)]
enum ClassifiedSlashDeleteRequest {
    Datasets(Vec<String>),
    Resources(Vec<String>),
    Mixed {
        dataset_args: Vec<String>,
        resource_args: Vec<String>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use kamu_resources::{ResourceTypeDescriptor, TypeUri};

    use super::{ANY_SELECTOR, ClassifiedSlashDeleteRequest, DeleteRequestResolver};

    #[test]
    fn test_classify_slash_request_routes_resource_prefixes_to_resources() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["vs/foo".to_owned(), "ss/bar".to_owned()],
            |prefix| matches!(prefix, "vs" | "ss"),
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Resources(args)
                if args == vec!["vs/foo".to_owned(), "ss/bar".to_owned()]
        );
    }

    #[test]
    fn test_classify_slash_request_preserves_unknown_prefixes_as_datasets() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["account/foo".to_owned()],
            |_| false,
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["account/foo".to_owned()]
        );
    }

    #[test]
    fn test_classify_slash_request_strips_dataset_pseudo_type_prefix() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["datasets/foo".to_owned(), "dataset/bar".to_owned()],
            |_| false,
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["foo".to_owned(), "bar".to_owned()]
        );
    }

    // A dataset alias may itself be account-qualified (`AccountName "/"
    // DatasetName`), so the escape hatch has to survive a second `/`. The
    // selector grammar allows only one, which is why the prefix is stripped
    // before the argument is scanned.
    #[test]
    fn test_classify_slash_request_strips_prefix_from_account_qualified_datasets() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec![
                "dataset/alice/foo".to_owned(),
                "datasets/bob/bar".to_owned(),
            ],
            |_| false,
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["alice/foo".to_owned(), "bob/bar".to_owned()]
        );
    }

    #[test]
    fn test_classify_slash_request_marks_mixed_requests() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["dataset/foo".to_owned(), "vs/bar".to_owned()],
            |prefix| prefix == "vs",
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Mixed { dataset_args, resource_args }
                if dataset_args == vec!["foo".to_owned()]
                    && resource_args == vec!["vs/bar".to_owned()]
        );
    }

    // The account-qualified alias carries a second `/`, which the resource
    // selector grammar rejects; it must still be routed as a dataset rather than
    // dragging the whole mixed request onto the resource path.
    #[test]
    fn test_classify_slash_request_marks_mixed_requests_with_account_qualified_datasets() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["dataset/alice/foo".to_owned(), "vs/bar".to_owned()],
            |prefix| prefix == "vs",
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Mixed { dataset_args, resource_args }
                if dataset_args == vec!["alice/foo".to_owned()]
                    && resource_args == vec!["vs/bar".to_owned()]
        );
    }

    #[test]
    fn test_classify_slash_request_routes_any_type_wildcard_to_resources() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["%/db-creds".to_owned()],
            |prefix| prefix == "%",
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Resources(args)
                if args == vec!["%/db-creds".to_owned()]
        );
    }

    #[test]
    fn test_classify_slash_request_accepts_uppercase_dataset_escape_hatch() {
        let request = DeleteRequestResolver::classify_slash_request_with(
            vec!["DATASETs/foo".to_owned()],
            |_| true,
        );

        assert_matches!(
            request,
            ClassifiedSlashDeleteRequest::Datasets(args)
                if args == vec!["foo".to_owned()]
        );
    }

    fn test_resource_types() -> Vec<ResourceTypeDescriptor> {
        vec![
            ResourceTypeDescriptor {
                canonical_selector: kamu_resources::ResourceSelectorName::new("variablesets")
                    .unwrap(),
                selector_aliases: vec![kamu_resources::ResourceSelectorName::new("vs").unwrap()],
                schema: TypeUri::new_unchecked("dev.kamu/variableset/v1"),
                list_columns: Vec::new(),
            },
            ResourceTypeDescriptor {
                canonical_selector: kamu_resources::ResourceSelectorName::new("secretsets")
                    .unwrap(),
                selector_aliases: vec![kamu_resources::ResourceSelectorName::new("ss").unwrap()],
                schema: TypeUri::new_unchecked("dev.kamu/secretset/v1"),
                list_columns: Vec::new(),
            },
        ]
    }

    #[test]
    fn test_matches_resource_target_prefix_matches_exact_names_case_insensitively() {
        let supported_resource_types = test_resource_types();

        assert!(DeleteRequestResolver::matches_resource_target_prefix_with(
            &supported_resource_types,
            "VS",
        ));
        // A dataset name may contain `%`; only the type half is wildcard-aware.
        assert!(!DeleteRequestResolver::matches_resource_target_prefix_with(
            &supported_resource_types,
            "my.dataset.%",
        ));
        assert!(!DeleteRequestResolver::matches_resource_target_prefix_with(
            &supported_resource_types,
            "S%",
        ));
    }

    // No descriptor matches a bare `%`, which is why `resolve` short-circuits on
    // it before the dataset fallback: otherwise `kamu delete %` would reach the
    // dataset path and glob-delete every dataset.
    #[test]
    fn test_matches_resource_target_prefix_does_not_match_bare_any_selector() {
        let supported_resource_types = test_resource_types();

        assert!(!DeleteRequestResolver::matches_resource_target_prefix_with(
            &supported_resource_types,
            ANY_SELECTOR,
        ));
    }

    #[test]
    fn test_matches_resource_slash_prefix_claims_every_wildcard_type_half() {
        let supported_resource_types = test_resource_types();

        assert!(DeleteRequestResolver::matches_resource_slash_prefix_with(
            &supported_resource_types,
            "%",
        ));
        // Rejected type wildcards stay on the resource path so the user gets the
        // resource-selector usage error rather than a dataset error.
        assert!(DeleteRequestResolver::matches_resource_slash_prefix_with(
            &supported_resource_types,
            "S%",
        ));
        assert!(DeleteRequestResolver::matches_resource_slash_prefix_with(
            &supported_resource_types,
            "unknown%",
        ));
        assert!(!DeleteRequestResolver::matches_resource_slash_prefix_with(
            &supported_resource_types,
            "unknown",
        ));
    }
}
