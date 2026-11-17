// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::TypeUri;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource type known at compile time, which parameterizes typed links such
/// as [`ResourceRefT`](super::ResourceRefT) and
/// [`ResourceHandleT`](super::ResourceHandleT).
pub trait ResourceType {
    /// Full schema URI of the type, e.g.
    /// `https://opendatafabric.org/schemas/auth/v1alpha1/Group`.
    fn type_uri_str() -> &'static str;

    /// Full schema URI of the type, e.g.
    /// `https://opendatafabric.org/schemas/auth/v1alpha1/Group`.
    fn type_uri() -> &'static TypeUri;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! impl_resource_type {
    ($ty:ident, $uri:literal $(,)?) => {
        $crate::__paste::paste! {
            static [<$ty:snake:upper _TYPE_URI_STR>]: &str = $uri;

            static [<$ty:snake:upper _TYPE_URI>]: std::sync::LazyLock<TypeUri> =
                std::sync::LazyLock::new(|| TypeUri::new_unchecked([<$ty:snake:upper _TYPE_URI_STR>]));

            impl $ty {
                pub const fn type_uri_str() -> &'static str {
                    [<$ty:snake:upper _TYPE_URI_STR>]
                }

                pub fn type_uri() -> &'static TypeUri {
                    &[<$ty:snake:upper _TYPE_URI>]
                }
            }

            impl $crate::resources::ResourceType for $ty {
                fn type_uri_str() -> &'static str {
                    [<$ty:snake:upper _TYPE_URI_STR>]
                }

                fn type_uri() -> &'static TypeUri {
                    &[<$ty:snake:upper _TYPE_URI>]
                }
            }
        }
    };
}

pub use impl_resource_type;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
