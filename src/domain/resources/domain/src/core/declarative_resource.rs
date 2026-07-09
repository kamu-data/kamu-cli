// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ResourceHeaders, ResourceID, ResourceSnapshot, ResourceStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource:
    Sized + Send + Sync + std::fmt::Debug + AsRef<Self::ResourceState>
{
    /// Resolved, stored shape — what `get`/`list` return and what a
    /// snapshot persists.
    type Spec: std::fmt::Debug + Send + Sync;
    /// Write-path shape decoded from a manifest/apply request, converted to
    /// `Spec` once at projection time. Identical to `Spec` unless the
    /// resource has server-side defaulting.
    type SpecInput: std::fmt::Debug + Send + Sync;
    type ResourceState: DeclarativeResourceState<Spec = Self::Spec>
        + TryFrom<ResourceSnapshot, Error = InternalError>
        + From<Self>;

    fn id(&self) -> &ResourceID {
        self.as_ref().id()
    }

    fn headers(&self) -> &ResourceHeaders {
        self.as_ref().headers()
    }

    fn spec(&self) -> &Self::Spec {
        self.as_ref().spec()
    }

    fn status(&self) -> &ResourceStatus {
        self.as_ref().status()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResourceState: Send + Sync + std::fmt::Debug {
    type Spec: std::fmt::Debug + Send + Sync;

    fn id(&self) -> &ResourceID;

    fn headers(&self) -> &ResourceHeaders;
    fn headers_mut(&mut self) -> &mut ResourceHeaders;

    fn spec(&self) -> &Self::Spec;
    fn spec_mut(&mut self) -> &mut Self::Spec;

    fn status(&self) -> &ResourceStatus;
    fn status_mut(&mut self) -> &mut ResourceStatus;

    fn into_parts(self) -> (ResourceID, ResourceHeaders, Self::Spec, ResourceStatus)
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Converts a resource's write-path `SpecInput` into its resolved, stored
/// `Spec`, filling any server-owned/defaulted fields. Mirrors
/// `ResourceHeadersExt::from_input`. For resource types where `Spec` and
/// `SpecInput` are the same type, this is the identity conversion.
pub trait ResourceSpecFromInput<TInput> {
    fn from_input(input: TInput) -> Self;

    /// Inverse of `from_input`, used only to re-derive a `SpecInput` from
    /// already-resolved server state (duplicate-create retry path). Not
    /// guaranteed lossless for types where `Spec` carries server-owned
    /// fields with no `SpecInput` counterpart.
    fn into_input(self) -> TInput;
}

/// Implements the identity `ResourceSpecFromInput<T> for T`. For every
/// resource except `VariableSet` (whose `SpecInput` is a distinct type),
/// `Spec` and `SpecInput` are the same type, so this avoids repeating the
/// same trivial impl per resource. Not a blanket `impl<T> ... for T`: that
/// form applies to *every* type in scope and collides with unrelated
/// same-named methods (e.g. `ResourceHeadersExt::from_input`).
#[macro_export]
macro_rules! declare_identity_resource_spec_from_input {
    ($ty:ty) => {
        impl $crate::ResourceSpecFromInput<$ty> for $ty {
            fn from_input(input: Self) -> Self {
                input
            }

            fn into_input(self) -> Self {
                self
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
