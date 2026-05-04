// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_resources_facade::{
    GetResourceError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    ResourceFacade,
    ResourceManifestFormat as FacadeResourceManifestFormat,
    ResourceRef,
};

use super::{CLIError, Command, common};
use crate::cli::GetOutputFormat;
use crate::resources::{
    ResourceFacadeFactory,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl GetResourceCommand {
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
            labels: &'a std::collections::BTreeMap<String, String>,
            annotations: &'a std::collections::BTreeMap<String, String>,
            generation: u64,
            created_at: &'a chrono::DateTime<chrono::Utc>,
            updated_at: &'a chrono::DateTime<chrono::Utc>,
            deleted_at: &'a Option<chrono::DateTime<chrono::Utc>>,
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
            last_reconciled_at: &'a Option<chrono::DateTime<chrono::Utc>>,
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
            last_reconciled_at: &'a Option<chrono::DateTime<chrono::Utc>>,
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

    async fn run_spec_view(
        &self,
        resource_facade: &dyn ResourceFacade,
        target: &ResourceTarget,
        format: FacadeResourceManifestFormat,
    ) -> Result<Option<String>, CLIError> {
        let rendered = resource_facade
            .render_manifest(RenderResourceManifestRequest {
                kind: target.kind.clone(),
                api_version: Some(target.api_version.clone()),
                account: None,
                resource_ref: ResourceRef::ById(target.uid),
                format,
            })
            .await;

        match rendered {
            Ok(rendered) => Ok(Some(rendered.manifest)),
            Err(
                RenderResourceManifestError::NameNotFound(_)
                | RenderResourceManifestError::UIDNotFound(_),
            ) if self.ignore_not_found => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn run_full_view(
        &self,
        resource_facade: &dyn ResourceFacade,
        target: &ResourceTarget,
        format: FacadeResourceManifestFormat,
    ) -> Result<Option<String>, CLIError> {
        let resource = resource_facade
            .get(kamu_resources_facade::GetResourceRequest {
                kind: target.kind.clone(),
                api_version: Some(target.api_version.clone()),
                account: None,
                resource_ref: ResourceRef::ById(target.uid),
            })
            .await;

        match resource {
            Ok(resource) => Ok(Some(self.render_full_resource(&resource, format)?)),
            Err(GetResourceError::NameNotFound(_) | GetResourceError::UIDNotFound(_))
                if self.ignore_not_found =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
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
            .resolve(syntax, resource_facade.as_ref(), self.ignore_not_found)
            .await?;

        match self.run_mode() {
            GetRunMode::Name => {
                for target in resolved_targets.targets {
                    self.print_name(&target)?;
                }
                Ok(())
            }
            GetRunMode::Manifest { format, spec } => {
                let mut rendered_items: Vec<String> = Vec::new();
                for target in resolved_targets.targets {
                    let item = if spec {
                        self.run_spec_view(resource_facade.as_ref(), &target, format)
                            .await?
                    } else {
                        self.run_full_view(resource_facade.as_ref(), &target, format)
                            .await?
                    };
                    if let Some(rendered) = item {
                        rendered_items.push(rendered);
                    }
                }
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
