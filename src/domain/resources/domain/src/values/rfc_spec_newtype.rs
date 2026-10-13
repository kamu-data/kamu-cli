// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Declares a thin newtype around an ODF RFC-generated DTO that lives outside
/// this crate, wired for serde via the DTO's YAML shadow proxy.
///
/// A bare `pub type` alias can't carry `Serialize`/`Deserialize`: the
/// generated DTOs have no serde of their own (only via
/// `odf::metadata::serde::yaml::…`), and implementing those foreign traits
/// directly on a foreign type — even through an alias — violates the orphan
/// rule. A local newtype is the only legal place to own that impl (and any
/// domain traits, e.g. `ResourceValidateSpec`/`ResourceLinterSpec`, this
/// crate wants to attach). This macro generates the newtype plus
/// `new`/`into_dto`/`Deref`/`DerefMut`/`TryFrom`/`From` boilerplate via
/// `#[serde(try_from = "…", into = "…")]`, matching the pattern
/// `odf::metadata::auth::AccountRef` uses for types codegen defines
/// natively — the difference here is this type is one layer removed from
/// codegen, so the proxy conversion has to be written by hand once, here.
#[macro_export]
macro_rules! declare_rfc_spec_newtype {
    ($name:ident, dto = $dto:ty, proxy = $proxy:ty, proxy_path = $proxy_path:literal) => {
        // `#[serde(try_from = "...")]` takes a string that serde's derive
        // parses as a type path in its own right (it does not need to name
        // a pre-existing local alias) — so the proxy's fully-qualified path
        // is passed once as a string literal (`$proxy_path`) for the
        // attribute, and once as a `:ty` fragment (`$proxy`) for the
        // `TryFrom`/`From` impls below. Caller keeps the two spellings in
        // sync (they name the same type).
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(try_from = $proxy_path, into = $proxy_path)]
        pub struct $name(pub(crate) $dto);

        impl $name {
            pub fn new(dto: $dto) -> Self {
                Self(dto)
            }

            pub fn into_dto(self) -> $dto {
                self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = $dto;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl TryFrom<$proxy> for $name {
            type Error = odf::metadata::errors::ValidationError;

            fn try_from(proxy: $proxy) -> Result<Self, Self::Error> {
                Ok(Self(proxy.try_into()?))
            }
        }

        impl From<$name> for $proxy {
            fn from(value: $name) -> Self {
                value.0.into()
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
