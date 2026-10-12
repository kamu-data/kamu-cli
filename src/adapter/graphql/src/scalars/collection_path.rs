// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: This type is kept for backward data compatibility.
//        For new code, please use `CollectionPathV2`

simple_string_scalar!(
    /// Path of a collection entry: rooted, slash-separated, with URL-encoded
    /// elements (e.g. `/foo%20bar/baz`)
    CollectionPath,
    kamu_datasets::CollectionPath,
    from_str
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
