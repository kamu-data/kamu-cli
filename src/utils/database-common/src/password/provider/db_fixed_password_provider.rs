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
use secrecy::SecretString;

use crate::{DatabaseCredentials, DatabasePasswordProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatabasePasswordProvider)]
pub struct DatabaseFixedPasswordProvider {
    #[component(explicit)]
    db_user_name: SecretString,

    #[component(explicit)]
    fixed_password: SecretString,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatabasePasswordProvider for DatabaseFixedPasswordProvider {
    async fn provide_credentials(&self) -> Result<Option<DatabaseCredentials>, InternalError> {
        Ok(Some(DatabaseCredentials {
            user_name: self.db_user_name.clone(),
            password: self.fixed_password.clone(),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
