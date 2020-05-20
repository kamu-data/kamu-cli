pub mod flatbuffers;
pub mod yaml;

use std::convert;
use std::error;
use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, SerdeError>;

pub trait Serializer<T> {
    fn serialize(&self, obj: &T, writer: &mut dyn io::Write) -> Result<()>;
}

pub trait Deserializer<T> {
    fn deserialize(&self, buf: &[u8]) -> Result<T>;
}

#[derive(Debug)]
pub struct SerdeError(Box<dyn error::Error>);

impl fmt::Display for SerdeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl error::Error for SerdeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl convert::From<io::Error> for SerdeError {
    fn from(err: io::Error) -> SerdeError {
        SerdeError(Box::new(err))
    }
}
