// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};

use format_utils::format_collection;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_format_numbers() {
    let numbers = vec![1, 2];

    assert_eq!("[1, 2]", format_collection(numbers));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_format_structs() {
    struct NumberStruct {
        value: i32,
    }

    impl Display for NumberStruct {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.value)
        }
    }

    let number_structs = vec![NumberStruct { value: 3 }, NumberStruct { value: 4 }];

    assert_eq!("[3, 4]", format_collection(number_structs));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
