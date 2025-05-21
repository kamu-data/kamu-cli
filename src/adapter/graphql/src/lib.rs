// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(int_roundings)]
#![feature(let_chains)]

pub mod extensions;
pub mod guards;
pub(crate) mod mutations;
pub(crate) mod prelude;
pub(crate) mod queries;
mod root;
#[cfg(any(feature = "testing", test))]
pub mod scalars;
#[cfg(not(any(feature = "testing", test)))]
pub mod scalars;
pub mod traits;
pub(crate) mod utils;

pub use guards::*;
pub use root::*;
