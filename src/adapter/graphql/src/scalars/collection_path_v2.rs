// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Collection entry paths are similar to HTTP path components. They are rooted
// (start with `/`), separated by forward slashes, with elements URL-encoded
// (e.g. `/foo%20bar/baz`)
simple_string_scalar!(CollectionPathV2, kamu_datasets::CollectionPathV2, try_new);

impl<'a> CollectionPathV2<'a> {
    pub fn into_v1_scalar(self) -> CollectionPath<'a> {
        let inner: kamu_datasets::CollectionPathV2 = self.into();
        CollectionPath::from(inner.into_v1())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
