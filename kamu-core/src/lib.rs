// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(backtrace)]
#![feature(try_blocks)]
#![feature(box_patterns)]
#![feature(with_options)]
#![feature(map_first_last)]
#![feature(exit_status_error)]
#![feature(iter_zip)]

pub mod domain;
pub mod infra;

// TODO: Put under feature flag
pub mod testing;
