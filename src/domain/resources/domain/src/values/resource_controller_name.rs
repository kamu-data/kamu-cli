// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stable name of a resource controller. Its bookkeeping rows are stored under
/// it, so renaming a released controller orphans them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResourceControllerName(Cow<'static, str>);

impl ResourceControllerName {
    pub const fn new_static(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    pub fn new(name: impl Into<String>) -> Self {
        Self(Cow::Owned(name.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ResourceControllerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
