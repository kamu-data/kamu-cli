// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::*;

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SystemDepgraphCommand {
    catalog: dill::CatalogWeakRef,
}

#[async_trait::async_trait(?Send)]
impl Command for SystemDepgraphCommand {
    async fn run(&self) -> Result<(), CLIError> {
        use std::io::Write;

        let text = dill::utils::plantuml::render(&self.catalog.upgrade());
        std::io::stdout().write_all(text.as_bytes()).int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
