// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use multiformats::stack_string::StackString;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_as_str() {
    let s = StackString::new(*b"hello", 5);
    assert_eq!("hello", s.as_str());

    let s = StackString::new(*b"hello", 3);
    assert_eq!("hel", s.as_str());

    let s = StackString::new(*b"hello", 0);
    assert_eq!("", s.as_str());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_make_ascii_lowercase() {
    let mut s = StackString::new(*b"hello", 5);
    s.make_ascii_lowercase();
    assert_eq!("hello", s.as_str());

    let mut s = StackString::new(*b"wOrLd", 5);
    s.make_ascii_lowercase();
    assert_eq!("world", s.as_str());

    let mut s = StackString::new(*b"\xCE\xBB 42 teST", 10);
    s.make_ascii_lowercase();
    assert_eq!("λ 42 test", s.as_str());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
