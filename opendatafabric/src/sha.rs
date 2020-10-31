use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::{Serialize, Serializer};
use std::convert::TryFrom;
use std::fmt;
use std::ops;

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Sha3_256([u8; 32]);

impl Sha3_256 {
    pub const LENGTH: usize = 32;

    pub fn new(data: [u8; Self::LENGTH]) -> Self {
        Self(data)
    }

    pub fn zero() -> Self {
        Self([0; Self::LENGTH])
    }

    pub fn from_str(s: &str) -> Result<Self, hex::FromHexError> {
        let mut slice: [u8; 32] = [0; 32];
        hex::decode_to_slice(s, &mut slice)?;
        Ok(Self(slice))
    }

    pub fn is_zero(&self) -> bool {
        for b in &self.0 {
            if *b != 0 {
                return false;
            }
        }
        return true;
    }

    pub fn as_array(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_string(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn short(&self) -> Sha3_256Short {
        Sha3_256Short(self, 8)
    }
}

///////////////////////////////////////////////////////////////////////////////

impl ops::Deref for Sha3_256 {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&str> for Sha3_256 {
    type Error = hex::FromHexError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl fmt::Debug for Sha3_256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Sha3_256")
            .field(&hex::encode(&self.0))
            .finish()
    }
}

impl fmt::Display for Sha3_256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(&self.0);
        write!(f, "{}", s)
    }
}

impl Serialize for Sha3_256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct Sha3_256Visitor;

impl<'de> Visitor<'de> for Sha3_256Visitor {
    type Value = Sha3_256;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a sha3-256 hex string")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Sha3_256::try_from(v).map_err(serde::de::Error::custom)
    }
}

// This is the trait that informs Serde how to deserialize MyMap.
impl<'de> Deserialize<'de> for Sha3_256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Sha3_256Visitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct Sha3_256Short<'a>(&'a Sha3_256, usize);

impl fmt::Display for Sha3_256Short<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = hex::encode(&self.0.as_array()[..self.1 / 2]);
        write!(f, "{}", s)
    }
}
