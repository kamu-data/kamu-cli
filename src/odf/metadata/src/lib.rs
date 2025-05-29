// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(let_chains)]

// Re-exports
pub use ed25519_dalek::SigningKey;
pub use multiformats::stack_string::{AsStackString, ToStackString};

pub mod engine;

#[cfg(any(feature = "testing", test))]
pub mod testing;

pub mod dtos;
pub mod errors;
pub mod formats;
pub mod identity;
pub mod serde;

pub mod metadata;
pub use dtos::*;
pub use errors::*;
pub use formats::*;
pub use identity::*;
pub use metadata::*;
