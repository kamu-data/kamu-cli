// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WebhookVerificationError {
    MissingHeader(&'static str),
    Malformed(&'static str),
    UnsupportedAlgorithm(String),
    UnsupportedComponent(String),
    /// A header listed in `Signature-Input` is not in the request
    MissingSignedHeader(String),
    /// The body does not hash to `Content-Digest`
    DigestMismatch,
    /// The HMAC of the signature base does not match `Signature`
    SignatureMismatch,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
