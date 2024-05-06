// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, Guard, Result};
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};

use crate::prelude::from_catalog;

////////////////////////////////////////////////////////////////////////////////////////

pub const ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE: &str = "Anonymous access forbidden";
pub const INVALID_ACCESS_TOKEN_MESSAGE: &str = "Invalid access token";
pub const EXPIRED_ACCESS_TOKEN_MESSAGE: &str = "Expired access token";

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
        if let CurrentAccountSubject::Anonymous(reason) = current_account_subject.as_ref() {
            Err(async_graphql::Error::new(match reason {
                AnonymousAccountReason::NoAuthenticationProvided => {
                    ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE
                }
                AnonymousAccountReason::AuthenticationInvalid => INVALID_ACCESS_TOKEN_MESSAGE,
                AnonymousAccountReason::AuthenticationExpired => EXPIRED_ACCESS_TOKEN_MESSAGE,
            }))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

pub const STAFF_ONLY_MESSAGE: &str = "Access restricted to administrators only";

pub struct AdminGuard {}

impl AdminGuard {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Guard for AdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

        match current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(a) if a.is_admin => Ok(()),
            _ => Err(async_graphql::Error::new(STAFF_ONLY_MESSAGE)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
