// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// use crate::{Reconciler, SecretSetResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetReconciler {
    // TODO
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
#[async_trait::async_trait]
impl Reconciler<SecretSetResource> for SecretSetReconciler {
    type Error = SecretSetReconcileError;

    async fn reconcile(&self, _resource: &mut SecretSetResource) -> Result<(), Self::Error> {
        unimplemented!()
    }
}
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SecretSetReconcileError {
    #[error(transparent)]
    Interna(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
