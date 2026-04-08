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
use kamu_resources::ResourceKindDescriptor;
use kamu_resources_facade::{
    GetResourceError,
    GetResourceRef,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    ResourceFacade,
    ResourceManifestFormat,
};

use super::{CLIError, Command, common};
use crate::resources::{
    ResourceFacadeFactory,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct GetResourceCommand {
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_kind_lookup_service: Arc<dyn ResourceKindLookupService>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    resource: String,

    #[dill::component(explicit)]
    name_or_id: String,

    #[dill::component(explicit)]
    output_format: crate::cli::ResourceManifestFormat,

    #[dill::component(explicit)]
    canonical: bool,

    #[dill::component(explicit)]
    ignore_not_found: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl GetResourceCommand {
    fn manifest_format(&self) -> ResourceManifestFormat {
        match self.output_format {
            crate::cli::ResourceManifestFormat::Json => ResourceManifestFormat::Json,
            crate::cli::ResourceManifestFormat::Yaml => ResourceManifestFormat::Yaml,
        }
    }

    fn resource_ref(&self) -> GetResourceRef {
        match uuid::Uuid::parse_str(&self.name_or_id) {
            Ok(uid) if uid.get_version() == Some(uuid::Version::Random) => {
                GetResourceRef::ById(kamu_resources::ResourceUID::new(uid))
            }
            _ => GetResourceRef::ByName(self.name_or_id.clone()),
        }
    }

    fn render_full_resource(
        &self,
        resource: &kamu_resources::ResourceView,
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

        match self.output_format {
            crate::cli::ResourceManifestFormat::Json => {
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

            crate::cli::ResourceManifestFormat::Yaml => {
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

    fn write_stdout(&self, rendered: &str) -> Result<(), CLIError> {
        let mut stdout = std::io::stdout();
        stdout.write_all(rendered.as_bytes()).int_err()?;
        Ok(())
    }

    async fn run_canonical(
        &self,
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: ResourceKindDescriptor,
        resource_ref: GetResourceRef,
    ) -> Result<(), CLIError> {
        let rendered = resource_facade
            .render_manifest(RenderResourceManifestRequest {
                kind: kind_descriptor.kind,
                api_version: Some(kind_descriptor.api_version),
                account: None,
                resource_ref,
                format: self.manifest_format(),
            })
            .await;

        match rendered {
            Ok(rendered) => self.write_stdout(&rendered.manifest),
            Err(
                RenderResourceManifestError::NameNotFound(_)
                | RenderResourceManifestError::UIDNotFound(_),
            ) if self.ignore_not_found => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn run_full_view(
        &self,
        resource_facade: &dyn ResourceFacade,
        kind_descriptor: ResourceKindDescriptor,
        resource_ref: GetResourceRef,
    ) -> Result<(), CLIError> {
        let resource = resource_facade
            .get(kamu_resources_facade::GetResourceRequest {
                kind: kind_descriptor.kind,
                api_version: Some(kind_descriptor.api_version),
                account: None,
                resource_ref,
            })
            .await;

        match resource {
            Ok(resource) => self.write_stdout(&self.render_full_resource(&resource)?),
            Err(GetResourceError::NameNotFound(_) | GetResourceError::UIDNotFound(_))
                if self.ignore_not_found =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for GetResourceCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let kind_descriptor = self
            .resource_kind_lookup_service
            .resolve_kind_descriptor(
                self.explicit_context_name.as_deref(),
                &self.resource,
                ResourceKindLookupErrorOptions::new("Unsupported get target"),
            )
            .await?;

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        if self.canonical {
            self.run_canonical(
                resource_facade.as_ref(),
                kind_descriptor,
                self.resource_ref(),
            )
            .await
        } else {
            self.run_full_view(
                resource_facade.as_ref(),
                kind_descriptor,
                self.resource_ref(),
            )
            .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
