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

/// Prepares a spec before it is planned, so before the first event holds it:
/// fills defaults, resolves references to identities, encrypts secrets.
///
/// An implementation keeps these rules, whatever the type's `SpecMutability`:
///
/// - What is equivalent to the current spec is returned as the current spec has it, or applying the
///   same manifest again would change the resource. The type decides what is equivalent. A
///   reference by ID or DID that matches the current one is equivalent without being resolved, even
///   if its object is gone; a reference by name is resolved, as the name may have another owner.
/// - Preparing a prepared spec again changes nothing.
/// - It has no side effects: it runs for plans too, and twice in an apply that loses a race for its
///   name.
/// - Defaults are filled here and not in [`ResourceSpecFromInput`](crate::ResourceSpecFromInput),
///   which runs on every replay: a changed default would change specs already stored.
/// - A default of a type with an immutable spec never changes: applying again a manifest that omits
///   the field would be rejected as a change.
/// - Its rejections and errors reach a principal who may not be allowed to read the account: a type
///   with a guard or a placement of its own is prepared before access is checked. They say nothing
///   about the current spec or other objects the principal could not read otherwise.
#[async_trait::async_trait]
pub trait ResourceSpecPreApply<R: DeclarativeResource>: Send + Sync {
    /// `new_spec` is not validated yet: the prepared spec is.
    ///
    /// `Ok(Rejected(_))` is for input that breaks a rule that `validate()` has
    /// no means to check, such as a reference to a missing object or an
    /// encrypted value that does not decrypt under the current key. `Err` is
    /// for technical failures.
    async fn pre_apply(
        &self,
        new_spec: R::SpecInput,
        context: ResourceSpecPreApplyContext<'_, R>,
    ) -> Result<ResourceSpecPreApplyOutcome<R>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceSpecPreApplyContext<'a, R: DeclarativeResource> {
    /// The account the resource is applied in
    pub account_id: &'a odf::AccountID,
    /// `None` for a resource to create
    pub maybe_current_spec: Option<&'a R::Spec>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ResourceSpecPreApplyOutcome<R: DeclarativeResource> {
    Prepared(R::SpecInput),
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
