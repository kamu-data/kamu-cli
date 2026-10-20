// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Steps a command defers until its transaction has committed and the outbox
/// messages it posted have been processed. For a transactional command each
/// step runs in a transaction of its own. Nothing runs when the command fails.
#[derive(Default)]
pub struct PostCommandActions {
    actions: Mutex<Vec<Box<dyn PostCommandAction>>>,
}

impl PostCommandActions {
    pub fn add(&self, action: impl PostCommandAction + 'static) {
        self.actions.lock().unwrap().push(Box::new(action));
    }

    pub fn take(&self) -> Vec<Box<dyn PostCommandAction>> {
        std::mem::take(&mut *self.actions.lock().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PostCommandAction: Send + Sync {
    async fn run(self: Box<Self>, catalog: dill::Catalog) -> Result<(), CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
