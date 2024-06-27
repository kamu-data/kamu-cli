// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: There has to be a crate for this
pub trait EnumWithVariants<E> {
    fn is_variant<V: VariantOf<E>>(&self) -> bool;
    fn into_variant<V: VariantOf<E>>(self) -> Option<V>;
    fn as_variant<V: VariantOf<E>>(&self) -> Option<&V>;
    fn as_variant_mut<V: VariantOf<E>>(&mut self) -> Option<&mut V>;
}

pub trait VariantOf<E>
where
    Self: Sized,
    Self: Into<E>,
{
    fn is_variant(e: &E) -> bool;
    fn into_variant(e: E) -> Option<Self>;
    fn as_variant(e: &E) -> Option<&Self>;
    fn as_variant_mut(e: &mut E) -> Option<&mut Self>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Use derive macro
#[macro_export]
macro_rules! impl_enum_with_variants {
    ($typ:ident) => {
        impl EnumWithVariants<$typ> for $typ {
            #[inline]
            fn is_variant<V: VariantOf<Self>>(&self) -> bool {
                V::is_variant(self)
            }

            #[inline]
            fn into_variant<V: VariantOf<Self>>(self) -> Option<V> {
                V::into_variant(self)
            }

            #[inline]
            fn as_variant<V: VariantOf<Self>>(&self) -> Option<&V> {
                V::as_variant(self)
            }

            #[inline]
            fn as_variant_mut<V: VariantOf<Self>>(&mut self) -> Option<&mut V> {
                V::as_variant_mut(self)
            }
        }
    };
}

// TODO: Make a derive macro
#[macro_export]
macro_rules! impl_enum_variant {
    ($enum_type:ident :: $variant:ident ( $variant_type:ident )) => {
        impl Into<$enum_type> for $variant_type {
            #[inline]
            fn into(self) -> $enum_type {
                $enum_type::$variant(self)
            }
        }

        impl VariantOf<$enum_type> for $variant_type {
            #[inline]
            fn is_variant(e: &$enum_type) -> bool {
                match e {
                    $enum_type::$variant(_) => true,
                    _ => false,
                }
            }

            #[inline]
            fn into_variant(e: $enum_type) -> Option<Self> {
                match e {
                    $enum_type::$variant(v) => Some(v),
                    _ => None,
                }
            }

            #[inline]
            fn as_variant(e: &$enum_type) -> Option<&Self> {
                match e {
                    $enum_type::$variant(v) => Some(v),
                    _ => None,
                }
            }

            #[inline]
            fn as_variant_mut(e: &mut $enum_type) -> Option<&mut Self> {
                match e {
                    $enum_type::$variant(v) => Some(v),
                    _ => None,
                }
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
