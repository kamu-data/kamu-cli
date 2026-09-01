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
use kamu_resources::TypeUri;
use kamu_resources_facade::{
    BatchResourceProblem,
    GetResourceError,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat as FacadeResourceManifestFormat,
};

use super::{CLIError, Command, common};
use crate::cli::GetOutputFormat;
use crate::resources::{
    ResourceFacadeFactory,
    ResourceLabelSelectorParser,
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
    revealed: bool,

    #[dill::component(explicit)]
    ignore_not_found: bool,

    #[dill::component(explicit)]
    max_results: NonZeroUsize,

    #[dill::component(explicit)]
    unbounded: bool,

    #[dill::component(explicit)]
    label_selectors: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl GetResourceCommand {
    const MATERIALIZATION_BATCH_SIZE: usize = 100;

    fn resolution_options(&self) -> Result<ResourceSelectionResolutionOptions, CLIError> {
        Ok(ResourceSelectionResolutionOptions {
            ignore_not_found: self.ignore_not_found,
            max_expanded_results: if self.unbounded {
                None
            } else {
                Some(self.max_results.get())
            },
            label_filter: ResourceLabelSelectorParser::parse(&self.label_selectors)?,
        })
    }

    fn spec_view(&self) -> kamu_resources_facade::SpecViewOpts {
        kamu_resources_facade::SpecViewOpts {
            revealed: self.revealed,
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
        resource: &kamu_resources::Resource,
        format: FacadeResourceManifestFormat,
    ) -> Result<String, CLIError> {
        #[serde_with::serde_as]
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RenderedResourceHeaders<'a> {
            id: &'a kamu_resources::ResourceID,
            #[serde_as(as = "odf::metadata::serde::yaml::auth::AccountHandle")]
            account: &'a odf::AccountHandle,
            name: &'a str,
            #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceLabels")]
            labels: &'a kamu_resources::ResourceLabels,
            #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceAnnotations")]
            annotations: &'a kamu_resources::ResourceAnnotations,
            generation: u64,
            created_at: &'a DateTime<Utc>,
            updated_at: &'a DateTime<Utc>,
            deleted_at: &'a Option<DateTime<Utc>>,
        }

        impl<'a> RenderedResourceHeaders<'a> {
            fn new(resource: &'a kamu_resources::Resource) -> Self {
                Self {
                    id: &resource.headers.id,
                    account: &resource.headers.account,
                    name: &resource.headers.name,
                    labels: &resource.headers.labels,
                    annotations: &resource.headers.annotations,
                    generation: resource.headers.generation,
                    created_at: &resource.headers.created_at,
                    updated_at: &resource.headers.updated_at,
                    deleted_at: &resource.headers.deleted_at,
                }
            }
        }

        #[derive(serde::Serialize)]
        struct RenderedResourceJson<'a> {
            #[serde(rename = "$schema")]
            schema: &'a str,
            headers: RenderedResourceHeaders<'a>,
            spec: &'a serde_json::Value,
            status: serde_json::Value,
        }

        #[derive(serde::Serialize)]
        struct RenderedResourceYaml<'a> {
            #[serde(rename = "$schema")]
            schema: &'a str,
            headers: RenderedResourceHeaders<'a>,
            spec: serde_yaml::Value,
            status: serde_yaml::Value,
        }

        match format {
            FacadeResourceManifestFormat::Json => {
                serde_json::to_string_pretty(&RenderedResourceJson {
                    schema: resource.schema.as_str(),
                    headers: RenderedResourceHeaders::new(resource),
                    spec: &resource.spec,
                    status: kamu_resources::resource_status_to_json(&resource.status),
                })
                .map_err(CLIError::critical)
            }

            FacadeResourceManifestFormat::Yaml => serde_yaml::to_string(&RenderedResourceYaml {
                schema: resource.schema.as_str(),
                headers: RenderedResourceHeaders::new(resource),
                spec: common::json_to_yaml_value(&resource.spec),
                status: common::json_to_yaml_value(&kamu_resources::resource_status_to_json(
                    &resource.status,
                )),
            })
            .map_err(CLIError::critical),
        }
    }

    fn print_name(&self, target: &ResourceTarget) -> Result<(), CLIError> {
        let mut stdout = std::io::stdout();
        writeln!(stdout, "{}/{}", target.canonical_selector, target.name).int_err()?;
        Ok(())
    }

    fn print_shadowed_selector_warning(selector_input: &str) {
        eprintln!(
            "Warning: selector `{selector_input}` ignored because a broader selector already \
             covers it"
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

        for (_schema, entries) in Self::group_targets_by_schema(targets) {
            for chunk in entries.chunks(Self::MATERIALIZATION_BATCH_SIZE) {
                let result = resource_facade
                    .render_manifests(Self::chunk_resource_refs(chunk), format, self.spec_view())
                    .await?;

                self.handle_lookup_problems(result.problems)?;

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

        for (_schema, entries) in Self::group_targets_by_schema(targets) {
            for chunk in entries.chunks(Self::MATERIALIZATION_BATCH_SIZE) {
                let result = resource_facade
                    .get(Self::chunk_resource_refs(chunk), self.spec_view())
                    .await?;

                self.handle_lookup_problems(result.problems)?;

                for success in result.successes {
                    let (original_index, _) = chunk[success.request_index];
                    rendered_items[original_index] =
                        Some(self.render_full_resource(&success.item, format)?);
                }
            }
        }

        Ok(rendered_items.into_iter().flatten().collect())
    }

    /// Builds one ref per already-resolved target.
    ///
    /// Targets carry their resolved schema, so the ref names the type by URI
    /// rather than by the selector the user typed — no second resolution, and
    /// no dependence on the chunk being single-type.
    fn chunk_resource_refs(chunk: &[(usize, &ResourceTarget)]) -> Vec<kamu_resources::ResourceRef> {
        chunk
            .iter()
            .map(|(_, target)| kamu_resources::ResourceRef {
                account: None,
                r#type: Some(target.schema.clone().into()),
                id: Some(target.id),
                did: None,
                name: None,
            })
            .collect()
    }

    fn group_targets_by_schema(
        targets: &[ResourceTarget],
    ) -> BTreeMap<TypeUri, Vec<(usize, &ResourceTarget)>> {
        let mut groups = BTreeMap::new();
        for (index, target) in targets.iter().enumerate() {
            groups
                .entry(target.schema.clone())
                .or_insert_with(Vec::new)
                .push((index, target));
        }
        groups
    }

    /// Fails on the first problem that `--ignore-not-found` does not excuse.
    ///
    /// Shared by the view and render paths: both address resources the same
    /// way, so a ref that fails one fails the other identically.
    fn handle_lookup_problems(
        &self,
        problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<(), CLIError> {
        for problem in problems {
            match problem.error {
                ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_)
                    if self.ignore_not_found => {}
                error => return Err(GetResourceError::LookupProblem(error).into()),
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
        if self.revealed && self.output_format == GetOutputFormat::Name {
            eprintln!("Warning: `--revealed` has no effect with `-o name`");
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
            .resolve(
                syntax,
                resource_facade.as_ref(),
                &self.resolution_options()?,
            )
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
