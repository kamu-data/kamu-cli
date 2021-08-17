use std::borrow;
use std::cmp;
use std::convert::TryInto;
use std::convert::{AsRef, TryFrom};
use std::fmt;
use std::marker::PhantomData;
use std::ops;

use super::grammar::DatasetIDGrammar;

////////////////////////////////////////////////////////////////////////////////
// Macro for defining zero-copy newtype idiom wrappers for strings
////////////////////////////////////////////////////////////////////////////////

macro_rules! newtype_str {
    ($ref_type:ident, $buf_type:ident, $parse:expr) => {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $ref_type(str);

        impl $ref_type {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> &Self {
                unsafe { &*(s.as_ref() as *const str as *const Self) }
            }

            pub fn try_from<S: AsRef<str> + ?Sized>(
                s: &S,
            ) -> Result<&Self, InvalidValue<$ref_type>> {
                match $parse(s.as_ref()) {
                    Some((_, "")) => Ok(Self::new_unchecked(s)),
                    _ => Err(InvalidValue::new(s)),
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl ops::Deref for $ref_type {
            type Target = str;

            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $ref_type {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<std::path::Path> for $ref_type {
            fn as_ref(&self) -> &std::path::Path {
                self.0.as_ref()
            }
        }

        impl ToOwned for $ref_type {
            type Owned = $buf_type;

            fn to_owned(&self) -> Self::Owned {
                Self::Owned::from(self)
            }
        }

        impl cmp::PartialEq<str> for $ref_type {
            fn eq(&self, other: &str) -> bool {
                &self.0 == other
            }
        }

        impl fmt::Display for $ref_type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $buf_type(String);

        impl $buf_type {
            pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> Self {
                Self::from($ref_type::new_unchecked(s))
            }
        }

        impl Into<String> for $buf_type {
            fn into(self) -> String {
                self.0
            }
        }

        impl From<&$ref_type> for $buf_type {
            fn from(id: &$ref_type) -> Self {
                Self(String::from(id as &str))
            }
        }

        impl std::str::FromStr for $buf_type {
            type Err = InvalidValue<$ref_type>;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::try_from(s)
            }
        }

        // TODO: Replace with AsRef matcher
        // See: https://github.com/rust-lang/rust/issues/50133
        impl TryFrom<&str> for $buf_type {
            type Error = InvalidValue<$ref_type>;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                let val = $ref_type::try_from(s)?;
                Ok(Self::from(val))
            }
        }

        impl TryFrom<String> for $buf_type {
            type Error = InvalidValue<$ref_type>;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                $ref_type::try_from(&s)?;
                Ok(Self(s))
            }
        }

        impl TryFrom<&std::ffi::OsString> for $buf_type {
            type Error = InvalidValue<$ref_type>;
            fn try_from(s: &std::ffi::OsString) -> Result<Self, Self::Error> {
                Self::try_from(s.to_str().unwrap())
            }
        }

        impl ops::Deref for $buf_type {
            type Target = $ref_type;

            fn deref(&self) -> &Self::Target {
                Self::Target::new_unchecked(&self.0)
            }
        }

        impl AsRef<str> for $buf_type {
            fn as_ref(&self) -> &str {
                self.0.as_ref()
            }
        }

        impl AsRef<std::path::Path> for $buf_type {
            fn as_ref(&self) -> &std::path::Path {
                self.0.as_ref()
            }
        }

        impl AsRef<$ref_type> for $buf_type {
            fn as_ref(&self) -> &$ref_type {
                $ref_type::new_unchecked(&self.0)
            }
        }

        impl borrow::Borrow<$ref_type> for $buf_type {
            fn borrow(&self) -> &$ref_type {
                self
            }
        }

        impl cmp::PartialEq<$ref_type> for $buf_type {
            fn eq(&self, other: &$ref_type) -> bool {
                self.0 == other.0
            }
        }

        impl cmp::PartialEq<str> for $buf_type {
            fn eq(&self, other: &str) -> bool {
                &self.0 == other
            }
        }

        impl fmt::Display for $buf_type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", &self.0)
            }
        }

        impl fmt::Display for InvalidValue<$ref_type> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "Invalid {}: {}", stringify!($ref_type), self.value)
            }
        }

        impl std::error::Error for InvalidValue<$ref_type> {}
    };
}

impl serde::Serialize for DatasetIDBuf {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for DatasetIDBuf {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(DatasetIDBufSerdeVisitor)
    }
}

struct DatasetIDBufSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for DatasetIDBufSerdeVisitor {
    type Value = DatasetIDBuf;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a DatasetID string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        DatasetIDBuf::try_from(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(DatasetID, DatasetIDBuf, DatasetIDGrammar::match_dataset_id);

impl DatasetID {
    pub fn as_dataset_ref(&self) -> &DatasetRef {
        DatasetRef::new_unchecked(&self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////

newtype_str!(Username, UsernameBuf, DatasetIDGrammar::match_username);

////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    RepositoryID,
    RepositoryBuf,
    DatasetIDGrammar::match_repository
);

////////////////////////////////////////////////////////////////////////////////

newtype_str!(
    DatasetRef,
    DatasetRefBuf,
    DatasetIDGrammar::match_dataset_ref
);

impl DatasetRef {
    pub fn is_local(&self) -> bool {
        self.0.chars().filter(|c| *c == '/').count() == 0
    }

    pub fn is_multitenant(&self) -> bool {
        self.0.chars().filter(|c| *c == '/').count() == 2
    }

    pub fn local_id(&self) -> &DatasetID {
        DatasetID::new_unchecked(self.0.rsplit('/').next().unwrap())
    }

    pub fn username(&self) -> Option<&Username> {
        let mut split = self.0.rsplit('/');
        split.next();
        let uname_or_repo = split.next();
        let maybe_repo = split.next();
        if maybe_repo.is_some() {
            uname_or_repo.map(|s| Username::new_unchecked(s))
        } else {
            None
        }
    }

    pub fn repository(&self) -> Option<&RepositoryID> {
        let mut split = self.0.rsplit('/');
        split.next();
        let uname_or_repo = split.next();
        if let Some(repo) = split.next() {
            Some(RepositoryID::new_unchecked(repo))
        } else {
            uname_or_repo.map(|s| RepositoryID::new_unchecked(s))
        }
    }

    pub fn as_local(&self) -> Option<&DatasetID> {
        let mut split = self.0.rsplit('/');
        let id = DatasetID::new_unchecked(split.next().unwrap());
        if split.next().is_none() {
            Some(id)
        } else {
            None
        }
    }
}

impl DatasetRefBuf {
    pub fn new(
        repo: Option<&RepositoryID>,
        username: Option<&Username>,
        local_id: &DatasetID,
    ) -> Self {
        let mut s = String::new();
        if let Some(r) = repo {
            s.push_str(r);
            s.push('/');
        }
        if let Some(u) = username {
            s.push_str(u);
            s.push('/');
        }
        s.push_str(local_id);
        Self::try_from(s).unwrap()
    }
}

impl From<&DatasetID> for DatasetRefBuf {
    fn from(id: &DatasetID) -> Self {
        Self(id.0.to_owned())
    }
}

impl From<DatasetIDBuf> for DatasetRefBuf {
    fn from(id: DatasetIDBuf) -> Self {
        Self(id.0)
    }
}

impl TryInto<DatasetIDBuf> for DatasetRefBuf {
    type Error = ();
    fn try_into(self) -> Result<DatasetIDBuf, Self::Error> {
        if self.is_local() {
            Ok(DatasetIDBuf(self.0))
        } else {
            Err(())
        }
    }
}

impl serde::Serialize for DatasetRefBuf {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for DatasetRefBuf {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(DatasetRefBufSerdeVisitor)
    }
}

struct DatasetRefBufSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for DatasetRefBufSerdeVisitor {
    type Value = DatasetRefBuf;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a DatasetRef string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        DatasetRefBuf::try_from(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct InvalidValue<T: ?Sized> {
    pub value: String,
    _ph: PhantomData<T>,
}

impl<T: ?Sized> InvalidValue<T> {
    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> Self {
        Self {
            value: s.as_ref().to_owned(),
            _ph: PhantomData,
        }
    }
}
