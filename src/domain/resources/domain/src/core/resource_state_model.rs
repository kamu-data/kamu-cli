// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::Projection;

use crate::{DeclarativeResourceState, ResourceEvent, ResourceState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStateModel {
    /// Mirrors `DeclarativeResource::Spec`/`SpecInput`.
    type Spec: std::fmt::Debug + Clone + Send + Sync;
    type SpecInput: std::fmt::Debug + Clone + Send + Sync;
    type State: DeclarativeResourceState<Spec = Self::Spec>
        + From<ResourceState<Self::Spec>>
        + Projection<Event = ResourceEvent<Self::SpecInput>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
