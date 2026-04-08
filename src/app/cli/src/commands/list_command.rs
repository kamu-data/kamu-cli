// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use kamu_datasets::DatasetRegistry;

use super::{CLIError, Command, ListDatasetsCommand, ListResourcesCommand, ListResourcesScope};
use crate::accounts;
use crate::output::OutputConfig;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    ResourceFacadeFactory,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASETS_TARGET: &str = "datasets";
const ALL_TARGET: &str = "all";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ListCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_statistics_service: Arc<dyn kamu_datasets::DatasetStatisticsService>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_kind_lookup_service: Arc<dyn ResourceKindLookupService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,

    #[dill::component(explicit)]
    current_account: accounts::CurrentAccountIndication,

    #[dill::component(explicit)]
    related_account: accounts::RelatedAccountIndication,

    #[dill::component(explicit)]
    target: Option<String>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    detail_level: u8,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ListCommand {
    fn mode(&self) -> ListMode {
        match self.target.as_deref() {
            None => ListMode::Datasets,
            Some(target) if target.eq_ignore_ascii_case(DATASETS_TARGET) => ListMode::Datasets,
            Some(target) if target.eq_ignore_ascii_case(ALL_TARGET) => ListMode::ResourcesAll,
            Some(_) => ListMode::ResourcesByKind,
        }
    }

    fn make_list_datasets_command(&self) -> ListDatasetsCommand {
        ListDatasetsCommand::new(
            self.tenancy_config,
            self.dataset_registry.clone(),
            self.dataset_statistics_service.clone(),
            self.remote_alias_reg.clone(),
            self.rebac_service.clone(),
            self.current_account.clone(),
            self.related_account.clone(),
            self.output_config.clone(),
            self.detail_level,
        )
    }

    async fn resolve_list_resources_command(&self) -> Result<ListResourcesCommand, CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let scope = match self.mode() {
            ListMode::ResourcesAll => ListResourcesScope::All,
            ListMode::ResourcesByKind => ListResourcesScope::ByKind(
                self.resource_kind_lookup_service
                    .resolve_kind_descriptor(
                        self.explicit_context_name.as_deref(),
                        self.target.as_deref().unwrap(),
                        ResourceKindLookupErrorOptions::new("Unsupported list target")
                            .with_additional_targets([DATASETS_TARGET, ALL_TARGET]),
                    )
                    .await?,
            ),
            ListMode::Datasets => unreachable!(),
        };

        Ok(ListResourcesCommand::new(
            resource_facade,
            self.resource_context_reporter.clone(),
            resolved_context,
            self.related_account.clone(),
            scope,
            self.output_config.clone(),
            self.detail_level,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ListCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        match self.mode() {
            ListMode::Datasets => {
                if self.explicit_context_name.is_some() {
                    return Err(CLIError::usage_error(
                        "--context is supported only when listing resources",
                    ));
                }

                self.make_list_datasets_command().validate_args().await
            }
            ListMode::ResourcesAll | ListMode::ResourcesByKind => {
                if self.related_account.is_explicit() {
                    return Err(CLIError::usage_error(
                        "Listing resources does not support --target-account or --all-accounts",
                    ));
                }

                Ok(())
            }
        }
    }

    async fn run(&self) -> Result<(), CLIError> {
        match self.mode() {
            ListMode::Datasets => self.make_list_datasets_command().run().await,
            ListMode::ResourcesAll | ListMode::ResourcesByKind => {
                self.resolve_list_resources_command().await?.run().await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListMode {
    Datasets,
    ResourcesAll,
    ResourcesByKind,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
