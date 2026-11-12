// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceHeaders;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource's generation and status generation: what a transition expects,
/// or what a controller's decision was based on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceVersion {
    pub generation: u64,
    pub status_generation: u64,
}

impl From<&ResourceHeaders> for ResourceVersion {
    fn from(headers: &ResourceHeaders) -> Self {
        Self {
            generation: headers.generation,
            status_generation: headers.status_generation,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
