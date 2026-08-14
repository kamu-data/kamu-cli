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

pub fn format_collection<T: Display>(collection_iter: impl IntoIterator<Item = T>) -> String {
    use std::fmt::Write;

    let mut iter = collection_iter.into_iter();
    let mut res = String::new();

    write!(res, "[").unwrap();

    if let Some(first) = iter.next() {
        write!(res, "{first}").unwrap();
        for item in iter {
            res.push_str(", ");
            write!(res, "{item}").unwrap();
        }
    }

    res.push(']');
    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
