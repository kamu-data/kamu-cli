// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(assert_matches)]
#![feature(try_blocks)]
#![feature(box_patterns)]
#![feature(exit_status_error)]
#![feature(error_generic_member_access)]
#![feature(let_chains)]
#![feature(iter_collect_into)]
#![feature(generic_arg_infer)]

// Re-exports
pub use kamu_core as domain;

mod engine;
mod services;
#[cfg(any(feature = "testing", test))]
pub mod testing;
mod use_cases;
pub mod utils;

pub use engine::*;
pub use services::*;
pub use use_cases::*;

mod dataset_config;

pub use dataset_config::*;
