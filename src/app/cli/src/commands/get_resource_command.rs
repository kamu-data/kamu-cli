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
    GetResourceRef,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    ResourceManifestFormat,
};

use super::{CLIError, Command};
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

        let rendered = resource_facade
            .render_manifest(RenderResourceManifestRequest {
                kind: kind_descriptor.kind,
                api_version: Some(kind_descriptor.api_version),
                account: None,
                resource_ref: self.resource_ref(),
                format: self.manifest_format(),
            })
            .await;

        match rendered {
            Ok(rendered) => {
                let mut stdout = std::io::stdout();
                stdout.write_all(rendered.manifest.as_bytes()).int_err()?;
                Ok(())
            }
            Err(
                RenderResourceManifestError::NameNotFound(_)
                | RenderResourceManifestError::UIDNotFound(_),
            ) if self.ignore_not_found => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
