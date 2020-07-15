use std::borrow;
use std::cmp;
use std::convert::TryFrom;
use std::fmt;
use std::ops;

use super::grammar::DatasetIDGrammar;

////////////////////////////////////////////////////////////////////////////////
// DatasetID (reference type)
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetID(str);

impl DatasetID {
    pub fn new_unchecked<S: AsRef<str> + ?Sized>(s: &S) -> &DatasetID {
        unsafe { &*(s.as_ref() as *const str as *const DatasetID) }
    }

    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> Result<&DatasetID, InvalidDatasetID> {
        match DatasetIDGrammar::match_dataset_id(s.as_ref()) {
            Some((_, "")) => Ok(DatasetID::new_unchecked(s)),
            _ => Err(InvalidDatasetID {
                invalid_id: String::from(s.as_ref()),
            }),
        }
    }
}

impl ops::Deref for DatasetID {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl ToOwned for DatasetID {
    type Owned = DatasetIDBuf;

    fn to_owned(&self) -> DatasetIDBuf {
        DatasetIDBuf::from(self)
    }
}

impl cmp::PartialEq for DatasetID {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl cmp::PartialEq<str> for DatasetID {
    fn eq(&self, other: &str) -> bool {
        &self.0 == other
    }
}

impl cmp::Eq for DatasetID {}

impl fmt::Display for DatasetID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////
// DatasetIDBuf (buffer type)
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetIDBuf(String);

impl DatasetIDBuf {
    pub fn new() -> Self {
        Self(String::new())
    }
}

impl Default for DatasetIDBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&DatasetID> for DatasetIDBuf {
    fn from(id: &DatasetID) -> Self {
        Self(String::from(id as &str))
    }
}

impl TryFrom<&str> for DatasetIDBuf {
    type Error = InvalidDatasetID;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let id = DatasetID::new(s)?;
        Ok(Self::from(id))
    }
}

impl ops::Deref for DatasetIDBuf {
    type Target = DatasetID;

    fn deref(&self) -> &DatasetID {
        DatasetID::new_unchecked(&self.0)
    }
}

impl borrow::Borrow<DatasetID> for DatasetIDBuf {
    fn borrow(&self) -> &DatasetID {
        self
    }
}

impl cmp::PartialEq for DatasetIDBuf {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl cmp::PartialEq<DatasetID> for DatasetIDBuf {
    fn eq(&self, other: &DatasetID) -> bool {
        self.0 == other.0
    }
}

impl cmp::PartialEq<str> for DatasetIDBuf {
    fn eq(&self, other: &str) -> bool {
        &self.0 == other
    }
}

impl cmp::Eq for DatasetIDBuf {}

impl fmt::Display for DatasetIDBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
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
// Errors
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct InvalidDatasetID {
    invalid_id: String,
}

impl fmt::Display for InvalidDatasetID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid DatasetID: {}", self.invalid_id)
    }
}
