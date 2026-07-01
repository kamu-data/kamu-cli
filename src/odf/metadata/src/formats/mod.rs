// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Re-export multiformats as ODF formats
pub use ed25519_dalek::SigningKey;
pub use multiformats::stack_string::{AsStackString, ToStackString};
pub use multiformats::{self, DidKey, Multicodec, Multihash, ParseError, PrivateKey, Signature};

mod did_odf;
#[cfg(feature = "did-pkh")]
mod did_pkh;
mod grammar;
mod macros;

#[cfg(feature = "sqlx")]
mod sqlx;

pub use did_odf::*;
#[cfg(feature = "did-pkh")]
pub use did_pkh::*;
pub use grammar::*;
pub(crate) use macros::*;
#[cfg(feature = "sqlx")]
pub(crate) use sqlx::*;
