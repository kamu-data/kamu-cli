// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;

use super::ManagedEntity;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManagedOperation: Sync + Send {
    fn track_entity(&self, managed_entity: Arc<dyn ManagedEntity>);

    async fn do_commit(self: Arc<Self>) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ManagedOperationRef {
    inner: Arc<dyn ManagedOperation>,
}

impl ManagedOperationRef {
    #[inline]
    pub fn new(inner: Arc<dyn ManagedOperation>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn track_entity(&self, managed_entity: Arc<dyn ManagedEntity>) {
        self.inner.track_entity(managed_entity);
    }

    #[inline]
    pub async fn do_commit(self) -> Result<(), InternalError> {
        self.inner.do_commit().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
