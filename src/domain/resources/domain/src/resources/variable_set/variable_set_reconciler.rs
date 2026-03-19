// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// use crate::{Reconciler, VariableSetResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetReconciler {
    // TODO
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
#[async_trait::async_trait]
impl Reconciler<VariableSetResource> for VariableSetReconciler {
    type Error = VariableSetReconcileError;

    async fn reconcile(&self, _resource: &mut VariableSetResource) -> Result<(), Self::Error> {
        unimplemented!()
    }
}
*/

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetReconcileError {
    #[error(transparent)]
    Interna(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
