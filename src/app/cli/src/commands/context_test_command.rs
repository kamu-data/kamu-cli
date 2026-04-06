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
use crate::resource_context::ResourceContextTestService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextTestCommand {
    resource_context_test_service: Arc<ResourceContextTestService>,

    #[dill::component(explicit)]
    name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextTestCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let result = self
            .resource_context_test_service
            .test_context(self.name.as_deref())
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
