// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, Guard, Result};
use kamu_core::CurrentAccountSubject;

use crate::prelude::from_catalog;

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoggedInGuard {}

impl LoggedInGuard {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Guard for LoggedInGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();
        if let CurrentAccountSubject::Anonymous(_) = current_account_subject.as_ref() {
            Err(async_graphql::Error::new("Anonymous access forbidden"))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
