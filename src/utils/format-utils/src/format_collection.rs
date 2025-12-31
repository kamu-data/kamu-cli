// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn format_collection<T: Display>(collection_it: impl IntoIterator<Item = T>) -> String {
    format!(
        "[{}]",
        collection_it
            .into_iter()
            .map(|a| a.to_string())
            .intersperse(", ".to_string())
            .collect::<String>()
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
