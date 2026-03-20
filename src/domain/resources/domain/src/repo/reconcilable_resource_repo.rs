// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{LoadError, SaveError};

use crate::ReconcilableResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ReconcilableResourceRepository<R: ReconcilableResource>: Send + Sync {
    async fn load(&self, id: &R::Identity) -> Result<R, LoadError<R::ResourceState>>;
    async fn save(&self, resource: &mut R) -> Result<(), SaveError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
