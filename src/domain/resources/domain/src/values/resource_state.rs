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

#[derive(Debug, Clone)]
pub struct ResourceState<
    TIdentity: std::fmt::Debug + Clone + Send + Sync,
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TStatus: std::fmt::Debug + Clone + Send + Sync,
> {
    pub id: TIdentity,
    pub metadata: ResourceMetadata,
    pub spec: TSpec,
    pub status: TStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
