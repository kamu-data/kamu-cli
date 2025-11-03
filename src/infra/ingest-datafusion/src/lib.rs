// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]

pub mod merge_strategies;
pub mod readers;
mod writer;

pub use kamu_core::ingest::*;
pub use merge_strategies::*;
pub use readers::*;
pub use writer::*;
