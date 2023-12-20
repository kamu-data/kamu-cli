// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]

mod did_key;
mod errors;
mod multibase;
mod multicodec;
mod multihash;
pub mod stack_string;

pub use did_key::*;
pub use errors::*;
pub use multibase::*;
pub use multicodec::*;
pub use multihash::*;

pub(crate) const MAX_VARINT_LEN: usize = 4;
