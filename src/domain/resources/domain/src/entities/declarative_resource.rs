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

pub trait DeclarativeResource {
    type Spec;
    type Status;

    fn metadata(&self) -> &ResourceMetadata;
    fn spec(&self) -> &Self::Spec;
    fn status(&self) -> &Self::Status;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<
    TIdentity: std::fmt::Debug + Clone,
    TSpec: std::fmt::Debug + Clone,
    TStatus: std::fmt::Debug + Clone,
> DeclarativeResource for ResourceState<TIdentity, TSpec, TStatus>
{
    type Spec = TSpec;
    type Status = TStatus;

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
