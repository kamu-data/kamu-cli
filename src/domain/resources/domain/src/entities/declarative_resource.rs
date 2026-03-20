// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResourceMetadata, ResourceState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource: Send + Sync {
    type Identity;
    type Spec;
    type Status;

    fn id(&self) -> &Self::Identity;
    fn metadata(&self) -> &ResourceMetadata;
    fn spec(&self) -> &Self::Spec;
    fn status(&self) -> &Self::Status;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<
    TIdentity: std::fmt::Debug + Clone + Send + Sync,
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TStatus: std::fmt::Debug + Clone + Send + Sync,
> DeclarativeResource for ResourceState<TIdentity, TSpec, TStatus>
{
    type Identity = TIdentity;
    type Spec = TSpec;
    type Status = TStatus;

    fn id(&self) -> &Self::Identity {
        &self.id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.spec
    }

    fn status(&self) -> &Self::Status {
        &self.status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
