// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceMetadata;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource: Send + Sync {
    type Identity: std::fmt::Debug + Send + Sync;
    type Spec: std::fmt::Debug + Send + Sync;
    type Status: std::fmt::Debug + Send + Sync;

    fn id(&self) -> &Self::Identity;
    fn metadata(&self) -> &ResourceMetadata;
    fn spec(&self) -> &Self::Spec;
    fn status(&self) -> &Self::Status;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
