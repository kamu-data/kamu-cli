// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::ResultIntoInternal;
use kamu_resources_facade::{
    BatchResourceProblem,
    RenderResourceManifestError,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat as FacadeResourceManifestFormat,
    ResourceRef,
};

use super::{CLIError, Command, common};
use crate::cli::GetOutputFormat;
use crate::resources::{
    ResourceFacadeFactory,
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntaxService,
    ResourceTarget,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct GetResourceCommand {
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_selection_syntax_service: Arc<dyn ResourceSelectionSyntaxService>,
    resource_selection_resolution_service: Arc<dyn ResourceSelectionResolutionService>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    args: Vec<String>,

    #[dill::component(explicit)]
    output_format: GetOutputFormat,

    #[dill::component(explicit)]
    spec: bool,

    #[dill::component(explicit)]
    ignore_not_found: bool,

    #[dill::component(explicit)]
    max_results: NonZeroUsize,

    #[dill::component(explicit)]
    unbounded: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl GetResourceCommand {
    const MATERIALIZATION_BATCH_SIZE: usize = 100;

    fn resolution_options(&self) -> ResourceSelectionResolutionOptions {
        ResourceSelectionResolutionOptions {
            ignore_not_found: self.ignore_not_found,
            max_expanded_results: if self.unbounded {
                None
            } else {
                Some(self.max_results.get())
            },
        }
    }

    fn run_mode(&self) -> GetRunMode {
        match self.output_format {
            GetOutputFormat::Name => GetRunMode::Name,
            GetOutputFormat::Json => GetRunMode::Manifest {
                format: FacadeResourceManifestFormat::Json,
                spec: self.spec,
            },
            GetOutputFormat::Yaml => GetRunMode::Manifest {
                format: FacadeResourceManifestFormat::Yaml,
                spec: self.spec,
            },
        }
    }

    fn render_full_resource(
        &self,
        resource: &kamu_resources::ResourceView,
        format: FacadeResourceManifestFormat,
    ) -> Result<String, CLIError> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RenderedResourceViewMetadata<'a> {
            uid: &'a kamu_resources::ResourceUID,
            account: &'a kamu_resources::ResourceViewAccount,
            name: &'a str,
            description: &'a Option<String>,
            labels: &'a BTreeMap<String, String>,
            annotations: &'a BTreeMap<String, String>,
            generation: u64,
            created_at: &'a DateTime<Utc>,
            updated_at: &'a DateTime<Utc>,
            deleted_at: &'a Option<DateTime<Utc>>,
        }

        impl<'a> RenderedResourceViewMetadata<'a> {
            fn new(resource: &'a kamu_resources::ResourceView) -> Self {
                Self {
                    uid: &resource.metadata.uid,
                    account: &resource.account,
                    name: &resource.metadata.name,
                    description: &resource.metadata.description,
                    labels: &resource.metadata.labels,
                    annotations: &resource.metadata.annotations,
                    generation: resource.metadata.generation,
                    created_at: &resource.metadata.created_at,
                    updated_at: &resource.metadata.updated_at,
                    deleted_at: &resource.metadata.deleted_at,
                }
            }
        }

        #[derive(serde::Serialize)]
        struct RenderedResourceViewJson<'a> {
            #[serde(rename = "apiVersion")]
            api_version: &'a str,
            kind: &'a str,
            metadata: RenderedResourceViewMetadata<'a>,
            #[serde(rename = "lastReconciledAt")]
            last_reconciled_at: &'a Option<DateTime<Utc>>,
            spec: &'a serde_json::Value,
            status: Option<&'a serde_json::Value>,
        }

        #[derive(serde::Serialize)]
        struct RenderedResourceViewYaml<'a> {
            #[serde(rename = "apiVersion")]
            api_version: &'a str,
            kind: &'a str,
            metadata: RenderedResourceViewMetadata<'a>,
            #[serde(rename = "lastReconciledAt")]
            last_reconciled_at: &'a Option<DateTime<Utc>>,
            spec: serde_yaml::Value,
            status: Option<serde_yaml::Value>,
        }

        match format {
            FacadeResourceManifestFormat::Json => {
                serde_json::to_string_pretty(&RenderedResourceViewJson {
                    api_version: &resource.api_version,
                    kind: &resource.kind,
                    metadata: RenderedResourceViewMetadata::new(resource),
                    last_reconciled_at: &resource.last_reconciled_at,
                    spec: &resource.spec,
                    status: resource.status.as_ref(),
                })
                .map_err(CLIError::critical)
            }

            FacadeResourceManifestFormat::Yaml => {
                serde_yaml::to_string(&RenderedResourceViewYaml {
                    api_version: &resource.api_version,
                    kind: &resource.kind,
                    metadata: RenderedResourceViewMetadata::new(resource),
                    last_reconciled_at: &resource.last_reconciled_at,
                    spec: common::json_to_yaml_value(&resource.spec),
                    status: resource.status.as_ref().map(common::json_to_yaml_value),
                })
                .map_err(CLIError::critical)
            }
        }
    }

    fn print_name(&self, target: &ResourceTarget) -> Result<(), CLIError> {
        let mut stdout = std::io::stdout();
        writeln!(stdout, "{}/{}", target.canonical_kind_name, target.name).int_err()?;
        Ok(())
    }

    fn print_shadowed_selector_warning(selector_input: &str) {
        eprintln!(
            "Warning: selector `{selector_input}` ignored because `all` selects the same scope"
        );
    }

    fn write_stdout(&self, rendered: &str) -> Result<(), CLIError> {
        let mut stdout = std::io::stdout();
        stdout.write_all(rendered.as_bytes()).int_err()?;
        Ok(())
    }

    async fn run_spec_views(
        &self,
        resource_facade: &dyn ResourceFacade,
        targets: &[ResourceTarget],
        format: FacadeResourceManifestFormat,
    ) -> Result<Vec<String>, CLIError> {
        let mut rendered_items = vec![None; targets.len()];

        for ((kind, api_version), entries) in Self::group_targets_by_descriptor(targets) {
            for chunk in entries.chunks(Self::MATERIALIZATION_BATCH_SIZE) {
                let result = resource_facade
                    .render_manifests(
                        ResourceBatchSelector {
                            account: None,
                            kind: kind.clone(),
                            api_version: Some(api_version.clone()),
                            resource_refs: chunk
                                .iter()
                                .map(|(_, target)| ResourceRef::ById(target.uid))
                                .collect(),
                        },
                        format,
                        kamu_resources_facade::SpecViewMode::Encrypted,
                    )
                    .await?;

                self.handle_render_manifest_problems(result.problems)?;

                for success in result.successes {
                    let (original_index, _) = chunk[success.request_index];
                    rendered_items[original_index] = Some(success.item.manifest);
                }
            }
        }

        Ok(rendered_items.into_iter().flatten().collect())
    }

    async fn run_full_views(
        &self,
        resource_facade: &dyn ResourceFacade,
        targets: &[ResourceTarget],
        format: FacadeResourceManifestFormat,
    ) -> Result<Vec<String>, CLIError> {
        let mut rendered_items = vec![None; targets.len()];

        for ((kind, api_version), entries) in Self::group_targets_by_descriptor(targets) {
            for chunk in entries.chunks(Self::MATERIALIZATION_BATCH_SIZE) {
                let result = resource_facade
                    .get_many(
                        ResourceBatchSelector {
                            account: None,
                            kind: kind.clone(),
                            api_version: Some(api_version.clone()),
                            resource_refs: chunk
                                .iter()
                                .map(|(_, target)| ResourceRef::ById(target.uid))
                                .collect(),
                        },
                        kamu_resources_facade::SpecViewMode::Encrypted,
                    )
                    .await?;

                self.handle_get_resource_problems(result.problems)?;

                for success in result.successes {
                    let (original_index, _) = chunk[success.request_index];
                    rendered_items[original_index] =
                        Some(self.render_full_resource(&success.item, format)?);
                }
            }
        }

        Ok(rendered_items.into_iter().flatten().collect())
    }

    fn group_targets_by_descriptor(
        targets: &[ResourceTarget],
    ) -> BTreeMap<(String, String), Vec<(usize, &ResourceTarget)>> {
        let mut groups = BTreeMap::new();
        for (index, target) in targets.iter().enumerate() {
            groups
                .entry((target.kind.clone(), target.api_version.clone()))
                .or_insert_with(Vec::new)
                .push((index, target));
        }
        groups
    }

    fn handle_get_resource_problems(
        &self,
        problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<(), CLIError> {
        for problem in problems {
            match problem.error {
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::UIDNotFound(_)
                    if self.ignore_not_found => {}
                error => {
                    return Err(
                        kamu_resources_facade::GetResourceError::LookupProblem(error).into(),
                    );
                }
            }
        }

        Ok(())
    }

    fn handle_render_manifest_problems(
        &self,
        problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<(), CLIError> {
        for problem in problems {
            match problem.error {
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::UIDNotFound(_)
                    if self.ignore_not_found => {}
                error => return Err(RenderResourceManifestError::LookupProblem(error).into()),
            }
        }

        Ok(())
    }

    fn output_rendered_items(
        &self,
        mut rendered_items: Vec<String>,
        format: FacadeResourceManifestFormat,
    ) -> Result<(), CLIError> {
        match rendered_items.len().cmp(&1) {
            std::cmp::Ordering::Equal => self.write_stdout(&rendered_items.remove(0)),
            std::cmp::Ordering::Greater => {
                let output = match format {
                    FacadeResourceManifestFormat::Json => Self::wrap_items_json(rendered_items)?,
                    FacadeResourceManifestFormat::Yaml => Self::wrap_items_yaml(rendered_items)?,
                };
                self.write_stdout(&output)
            }
            std::cmp::Ordering::Less => Ok(()),
        }
    }

    fn wrap_items_json(rendered_items: Vec<String>) -> Result<String, CLIError> {
        let values: Vec<serde_json::Value> = rendered_items
            .into_iter()
            .map(|s| serde_json::from_str(&s))
            .collect::<Result<_, _>>()
            .map_err(CLIError::critical)?;
        #[derive(serde::Serialize)]
        struct ItemList {
            items: Vec<serde_json::Value>,
        }
        serde_json::to_string_pretty(&ItemList { items: values }).map_err(CLIError::critical)
    }

    fn wrap_items_yaml(rendered_items: Vec<String>) -> Result<String, CLIError> {
        let values: Vec<serde_yaml::Value> = rendered_items
            .into_iter()
            .map(|s| serde_yaml::from_str(&s))
            .collect::<Result<_, _>>()
            .map_err(CLIError::critical)?;
        #[derive(serde::Serialize)]
        struct ItemList {
            items: Vec<serde_yaml::Value>,
        }
        serde_yaml::to_string(&ItemList { items: values }).map_err(CLIError::critical)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for GetResourceCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.spec && self.output_format == GetOutputFormat::Name {
            return Err(CLIError::usage_error(
                "`--spec` cannot be used with `-o name`",
            ));
        }
        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let syntax = self
            .resource_selection_syntax_service
            .parse_get_args(self.explicit_context_name.as_deref(), &self.args)
            .await?;

        for shadowed_selector in &syntax.shadowed_selectors {
            Self::print_shadowed_selector_warning(&shadowed_selector.selector_input);
        }

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let resolved_targets = self
            .resource_selection_resolution_service
            .resolve(syntax, resource_facade.as_ref(), self.resolution_options())
            .await?;

        match self.run_mode() {
            GetRunMode::Name => {
                for target in resolved_targets.targets {
                    self.print_name(&target)?;
                }
                Ok(())
            }
            GetRunMode::Manifest { format, spec } => {
                let rendered_items = if spec {
                    self.run_spec_views(resource_facade.as_ref(), &resolved_targets.targets, format)
                        .await?
                } else {
                    self.run_full_views(resource_facade.as_ref(), &resolved_targets.targets, format)
                        .await?
                };
                self.output_rendered_items(rendered_items, format)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum GetRunMode {
    Name,
    Manifest {
        format: FacadeResourceManifestFormat,
        spec: bool,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
