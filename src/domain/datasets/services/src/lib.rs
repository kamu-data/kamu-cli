// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(let_chains)]
#![feature(trait_upcasting)]
#![feature(error_generic_member_access)]

// Re-exports
pub use kamu_datasets as domain;

mod jobs;
mod messages;
mod services;
mod use_cases;
pub(crate) mod utils;

#[cfg(feature = "testing")]
pub mod testing;

pub use jobs::*;
pub use messages::*;
pub use services::*;
pub use use_cases::*;
