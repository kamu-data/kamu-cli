// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{GetEventsError, LoadError};

use crate::{ReconcilableEventSourcedResource, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceAggregateLoader<R: ReconcilableEventSourcedResource>: Send + Sync {
    async fn load(&self, id: &ResourceID) -> Result<R, LoadError<R::ResourceState>>;

    async fn load_many(
        &self,
        ids: &[ResourceID],
    ) -> Result<Vec<Result<R, LoadError<R::ResourceState>>>, GetEventsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
