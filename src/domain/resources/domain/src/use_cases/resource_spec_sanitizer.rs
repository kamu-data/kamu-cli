// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ApplyResourceRejection, DeclarativeResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceSpecSanitizer<R: DeclarativeResource>: Send + Sync {
    /// Sanitizes `new_spec` (e.g. encrypting plaintext secrets) before the
    /// planner sees it. `Ok(Rejected(_))` is for input that is well-formed
    /// but fails a business rule the sanitizer alone can check (e.g. an
    /// already-encrypted value that does not decrypt under the current key) —
    /// same business-rejection path as `ResourceValidateSpec`, just reachable
    /// only here because it needs key material `validate()` doesn't have.
    /// `Err` is reserved for technical failures (e.g. the encryption key
    /// itself is unavailable/misconfigured).
    async fn sanitize_new_spec(
        &self,
        new_spec: R::SpecInput,
        maybe_current_spec: Option<&R::Spec>,
    ) -> Result<SanitizeSpecOutcome<R>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum SanitizeSpecOutcome<R: DeclarativeResource> {
    Sanitized(R::SpecInput),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
