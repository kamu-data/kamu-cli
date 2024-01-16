// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(error_in_core)]

use std::backtrace::Backtrace;
use std::error::Error;

use internal_error::*;

#[derive(Debug, thiserror::Error)]
#[error("A")]
struct A {
    backtrace: Backtrace,
}

#[test]
fn test_reuses_backtrace_one_layer() {
    let outer = A {
        backtrace: Backtrace::capture(),
    }
    .int_err();

    let inner = outer.source().unwrap();

    let inner_bt = core::error::request_ref::<Backtrace>(&inner).unwrap();
    let outer_bt = core::error::request_ref::<Backtrace>(&outer).unwrap();

    assert!(std::ptr::eq(inner_bt, outer_bt));
}

#[derive(Debug, thiserror::Error)]
#[error("B")]
struct B {
    #[source]
    #[backtrace]
    source: Box<dyn std::error::Error + Send + Sync>,
}

#[test]
fn test_reuses_backtrace_two_layers() {
    let outer = B {
        source: A {
            backtrace: Backtrace::capture(),
        }
        .into(),
    }
    .int_err();

    let mid = outer.source().unwrap();
    let inner = mid.source().unwrap();

    let inner_bt = core::error::request_ref::<Backtrace>(&inner).unwrap();
    let outer_bt = core::error::request_ref::<Backtrace>(&outer).unwrap();

    assert!(std::ptr::eq(inner_bt, outer_bt));
}

#[derive(Debug, thiserror::Error)]
#[error("C")]
struct C {
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

#[test]
fn test_creates_backtrace_when_unavailable() {
    let outer = C {
        source: A {
            backtrace: Backtrace::capture(),
        }
        .into(),
    }
    .int_err();

    let mid = outer.source().unwrap();
    let inner = mid.source().unwrap();

    let inner_bt = core::error::request_ref::<Backtrace>(&inner).unwrap();
    let outer_bt = core::error::request_ref::<Backtrace>(&outer).unwrap();

    assert!(!std::ptr::eq(inner_bt, outer_bt));
}
