// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_parse_error {
    ($typ:ident) => {
        impl ::multiformats::Multiformat for $typ {
            fn format_name() -> &'static str {
                stringify!($typ)
            }
        }
    };
}

pub(crate) use impl_parse_error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with AsRef matcher
// This is a workaround for: https://github.com/rust-lang/rust/issues/50133
macro_rules! impl_try_from_str {
    ($typ:ident) => {
        impl std::convert::TryFrom<&str> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s)
            }
        }

        impl std::convert::TryFrom<String> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl std::convert::TryFrom<&String> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &String) -> Result<Self, Self::Error> {
                <Self as std::str::FromStr>::from_str(s.as_str())
            }
        }

        impl std::convert::TryFrom<&std::ffi::OsString> for $typ {
            type Error = ::multiformats::ParseError<$typ>;
            fn try_from(s: &std::ffi::OsString) -> Result<Self, Self::Error> {
                // TODO: May not always be convertible
                <Self as std::str::FromStr>::from_str(s.to_str().unwrap())
            }
        }
    };
}

pub(crate) use impl_try_from_str;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_serde {
    ($typ:ident, $visitor:ident) => {
        impl serde::Serialize for $typ {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> serde::Deserialize<'de> for $typ {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                deserializer.deserialize_string($visitor)
            }
        }

        struct $visitor;

        impl<'de> serde::de::Visitor<'de> for $visitor {
            type Value = $typ;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a {} string", stringify!($typ))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                $typ::try_from(v).map_err(serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use impl_serde;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! newtype_str {
    ($typ:ident, $parse:expr, $visitor:ident) => {
        #[derive(Debug, Clone, Eq)]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        pub struct $typ(std::sync::Arc<str>);

        impl $typ {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> Self {
                Self(std::sync::Arc::from(s.as_ref()))
            }

            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }

            pub fn into_inner(self) -> std::sync::Arc<str> {
                self.0
            }

            pub fn from_inner_unchecked(s: std::sync::Arc<str>) -> Self {
                Self(s)
            }

            pub fn into_lowercase(s: &str) -> std::borrow::Cow<'_, str> {
                let bytes = s.as_bytes();
                if !bytes.iter().any(u8::is_ascii_uppercase) {
                    std::borrow::Cow::Borrowed(s)
                } else {
                    std::borrow::Cow::Owned(s.to_ascii_lowercase())
                }
            }
        }

        impl From<$typ> for String {
            fn from(v: $typ) -> String {
                (*v.0).into()
            }
        }

        impl From<&$typ> for String {
            fn from(v: &$typ) -> String {
                (*v.0).into()
            }
        }

        impl From<&$typ> for $typ {
            fn from(v: &$typ) -> Self {
                v.clone()
            }
        }

        impl std::str::FromStr for $typ {
            type Err = ::multiformats::ParseError<$typ>;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match $parse(s) {
                    Some((_, "")) => Ok(Self::new_unchecked(s)),
                    _ => Err(::multiformats::ParseError::new(s)),
                }
            }
        }

        impl PartialEq for $typ {
            fn eq(&self, other: &$typ) -> bool {
                self.eq_ignore_ascii_case(other)
            }
        }

        impl std::hash::Hash for $typ {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                Self::into_lowercase(&self.0).hash(state);
            }
        }

        impl PartialOrd for $typ {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $typ {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                Self::into_lowercase(self).cmp(&Self::into_lowercase(other))
            }
        }

        impl std::ops::Deref for $typ {
            type Target = str;

            fn deref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<str> for $typ {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<std::path::Path> for $typ {
            fn as_ref(&self) -> &std::path::Path {
                (*self.0).as_ref()
            }
        }

        impl std::cmp::PartialEq<&str> for $typ {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl std::cmp::PartialEq<&str> for &$typ {
            fn eq(&self, other: &&str) -> bool {
                *self.0 == **other
            }
        }

        impl std::fmt::Display for $typ {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        impl_try_from_str!($typ);

        impl_serde!($typ, $visitor);

        impl_parse_error!($typ);
    };
}

pub(crate) use newtype_str;
