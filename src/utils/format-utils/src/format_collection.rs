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

// todo return std::fmt::Result?
pub fn format_collection<T: Display>(collection_iter: impl IntoIterator<Item = T>) -> String {
    use std::fmt::Write;

    let mut iter = collection_iter.into_iter();
    let mut result = String::from("[");

    if let Some(first) = iter.next() {
        write!(result, "{first}").unwrap();
        for item in iter {
            result.push_str(", ");
            write!(result, "{item}").unwrap();
        }
    }

    result.push(']');
    result
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
