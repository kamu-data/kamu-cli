// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Verifies a received webhook the way a customer's receiver does: it trusts
//! nothing but the request, its own public URL and the subscription secret,
//! and rebuilds the signature base from what `Signature-Input` declares.

mod content_digest;
mod errors;
mod receiver;
mod signature;
mod signature_input;
#[cfg(test)]
mod tests;

use content_digest::*;
pub(crate) use errors::*;
pub(crate) use receiver::*;
use signature::*;
use signature_input::*;
