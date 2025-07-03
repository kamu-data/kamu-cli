// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::CurrentAccountSubject;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthPolicyService: Sync + Send {
    fn check(&self) -> Result<(), AuthPolicyCheckError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthPolicyService)]
pub struct AllowAnonymousAuthPolicyServiceImpl {}

impl AuthPolicyService for AllowAnonymousAuthPolicyServiceImpl {
    fn check(&self) -> Result<(), AuthPolicyCheckError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthPolicyService)]
pub struct RestrictAnonymousAuthPolicyServiceImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
}

impl AuthPolicyService for RestrictAnonymousAuthPolicyServiceImpl {
    fn check(&self) -> Result<(), AuthPolicyCheckError> {
        match &*self.current_account_subject {
            CurrentAccountSubject::Logged(_) => Ok(()),
            CurrentAccountSubject::Anonymous(_) => {
                Err(AuthPolicyCheckError::AnonymousAccountRestricted)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AuthPolicyCheckError {
    #[error("Anonymous account is restricted")]
    AnonymousAccountRestricted,
}
