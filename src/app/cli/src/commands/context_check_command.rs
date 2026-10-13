// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use crate::resource_context::{ResourceContextRegistryService, ResourceContextTestService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextCheckCommand {
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    resource_context_test_service: Arc<ResourceContextTestService>,

    #[dill::component(explicit)]
    name: Option<String>,

    #[dill::component(explicit)]
    all: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextCheckCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.all && self.name.is_some() {
            return Err(CLIError::usage_error(
                "Specify either a context name or `--all`, but not both",
            ));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        if self.all {
            return self.run_all().await;
        }

        let result = self
            .resource_context_test_service
            .test_context_and_persist(self.name.as_deref())
            .await?;

        if result.is_healthy() {
            eprintln!("{}", console::style(result.summary()).green().bold());
            return Ok(());
        }

        let mut message = result.summary();
        if let Some(recommendation) = result.recommendation {
            message.push_str(". ");
            message.push_str(&recommendation);
        }

        Err(CLIError::usage_error(message))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ContextCheckCommand {
    async fn run_all(&self) -> Result<(), CLIError> {
        let contexts = self
            .resource_context_registry_service
            .list_effective_contexts_with_scope();

        if contexts.is_empty() {
            eprintln!(
                "{}",
                console::style("No remote contexts configured")
                    .yellow()
                    .bold()
            );
            return Ok(());
        }

        let mut has_failures = false;
        let results = self
            .resource_context_test_service
            .test_all_contexts_and_persist()
            .await?;

        for result in results {
            if result.is_healthy() {
                eprintln!("{}", console::style(result.summary()).green().bold());
            } else {
                has_failures = true;
                eprintln!("{}", console::style(result.summary()).yellow().bold());
                if let Some(recommendation) = result.recommendation {
                    eprintln!("{recommendation}");
                }
            }
        }

        if has_failures {
            Err(CLIError::usage_error(
                "One or more contexts are unreachable or require re-authentication",
            ))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
