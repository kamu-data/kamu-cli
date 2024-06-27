// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::InternalError;
use secrecy::Secret;

use crate::DatabasePasswordProvider;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseFixedPasswordProvider {
    fixed_password: Secret<String>,
}

impl DatabaseFixedPasswordProvider {
    pub fn new(fixed_password: Secret<String>) -> Self {
        Self { fixed_password }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseFixedPasswordProvider {
    async fn provide_password(&self) -> Result<Option<Secret<String>>, InternalError> {
        Ok(Some(self.fixed_password.clone()))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
